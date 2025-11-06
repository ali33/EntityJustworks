using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace EntityJustWorks.SQL.Core
{

    public class DynamicCommandExecutor : ICommandExecutor
    {
        private readonly IDbConnection _connection;

        public DynamicCommandExecutor(IDbConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        #region Basic CRUD (giữ nguyên)
        public async Task<int> InsertAsync(string table, IDictionary<string, object> values)
        {
            if (values == null || values.Count == 0)
                throw new ArgumentException("values cannot be empty");

            string columns = string.Join(",", values.Keys.Select(k=> QuotedName(k)));
            string parameters = string.Join(",", values.Keys.Select(k => "@" + k));
            string sql = $"INSERT INTO {QuotedName(table)} ({columns}) VALUES ({parameters});";

            return await ExecuteNonQueryAsync(sql, values);
        }

        public async Task<int> UpdateAsync(string table, IDictionary<string, object> values, IDictionary<string, object> keys)
        {
            if (values == null || values.Count == 0)
                throw new ArgumentException("values cannot be empty");
            if (keys == null || keys.Count == 0)
                throw new ArgumentException("keys cannot be empty");

            string setClause = string.Join(",", values.Keys.Select(k => $"{QuotedName(k)}=@{k}"));
            string whereClause = string.Join(" AND ", keys.Keys.Select(k => $"{QuotedName(k)}=@key_{k}"));

            string sql = $"UPDATE {QuotedName(table)} SET {setClause} WHERE {whereClause};";

            var parameters = new Dictionary<string, object>(values);
            foreach (var kv in keys)
                parameters["key_" + kv.Key] = kv.Value;

            return await ExecuteNonQueryAsync(sql, parameters);
        }

        public async Task<int> UpsertAsync(string table, IDictionary<string, object> values, IDictionary<string, object> keys)
        {
            // Chuẩn hóa key để tránh trùng
            var all = new Dictionary<string, object>(values);
            foreach (var kv in keys)
                all[kv.Key] = kv.Value;

            // Tùy hệ quản trị có thể viết riêng MERGE
            string mergeSql = $@"
MERGE {QuotedName(table)} AS target
USING (SELECT {string.Join(",", all.Keys.Select(k => "@" + k))}) AS source({string.Join(",", all.Keys.Select(k => QuotedName(k)))})
ON {string.Join(" AND ", keys.Keys.Select(k => $"target.{QuotedName(k)}=source.{QuotedName(k)}"))}
WHEN MATCHED THEN
    UPDATE SET {string.Join(",", values.Keys.Select(k => $"target.{QuotedName(k)}=source.{QuotedName(k)}"))}
WHEN NOT MATCHED THEN
    INSERT ({string.Join(",", all.Keys.Select(k=>QuotedName(k)))}) VALUES ({string.Join(",", all.Keys.Select(k => "source." + QuotedName(k)))});";

            return await ExecuteNonQueryAsync(mergeSql, all);
        }

        public async Task<int> DeleteAsync(string table, IDictionary<string, object> keys)
        {
            if (keys == null || keys.Count == 0)
                throw new ArgumentException("keys cannot be empty");

            string whereClause = string.Join(" AND ", keys.Keys.Select(k => $"{QuotedName(k)}=@{k}"));
            string sql = $"DELETE FROM {QuotedName(table)} WHERE {whereClause};";

            return await ExecuteNonQueryAsync(sql, keys);
        }
        #endregion

        #region 🔹 InsertRange
        public async Task<int> InsertRangeAsync(string table, IEnumerable<IDictionary<string, object>> items)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            var list = items.ToList();
            if (list.Count == 0) return 0;

            // Lấy cột từ item đầu tiên
            var first = list.First();
            string columns = string.Join(",", first.Keys.Select(c => QuotedName(c)));

            // Nếu SQL Server: build multi-value insert
            // INSERT INTO Table (A,B) VALUES (@A0,@B0),(@A1,@B1),...
            var sql = $"INSERT INTO {QuotedName(table)} ({columns}) VALUES ";
            var allParams = new Dictionary<string, object>();
            var valueParts = new List<string>();

            int idx = 0;
            foreach (var item in list)
            {
                var paramNames = item.Keys.Select(k => "@" + k + idx).ToArray();
                valueParts.Add("(" + string.Join(",", paramNames) + ")");
                foreach (var kv in item)
                    allParams[kv.Key + idx] = kv.Value;
                idx++;
            }

            sql += string.Join(",", valueParts) + ";";
            return await ExecuteNonQueryAsync(sql, allParams);
        }
        #endregion

        private string QuotedName(string str)
        {
            if (string.IsNullOrEmpty(str))
                throw new ArgumentNullException(nameof(str));

            // Nếu đã được bọc rồi thì trả về nguyên
            if (str.StartsWith("[") && str.EndsWith("]"))
                return str;

            // Escape ký tự ']' bằng ']]' để tránh lỗi
            return "[" + str.Replace("]", "]]") + "]";
        }


        #region 🔹 UpsertRange
        public async Task<int> UpsertRangeAsync(string table, IEnumerable<IDictionary<string, object>> items, IEnumerable<string> keyColumns)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            if (keyColumns == null) throw new ArgumentNullException(nameof(keyColumns));

            var list = items.ToList();
            if (list.Count == 0) return 0;

            // Giả định tất cả items có cùng schema
            var allColumns = list.First().Keys.ToList();
            var keyList = keyColumns.ToList();
            var nonKeys = allColumns.Except(keyList).ToList();

            // SQL Server MERGE không hỗ trợ nhiều record từ VALUES, nên ta tạo bảng tạm dữ liệu nguồn
            // -> Ta build chuỗi SELECT UNION ALL
            var paramMap = new Dictionary<string, object>();
            var selects = new List<string>();
            int i = 0;

            foreach (var item in list)
            {
                var cols = allColumns.Select(c => $"@{c}_{i} AS {QuotedName(c)}");
                selects.Add("SELECT " + string.Join(",", cols));
                foreach (var kv in item)
                    paramMap[kv.Key + "_" + i] = kv.Value;
                i++;
            }

            string sourceAlias = $"({string.Join(" UNION ALL ", selects)}) AS source";
            string onClause = string.Join(" AND ", keyList.Select(k => $"target.{QuotedName(k)}=source.{QuotedName(k)}"));
            string updateClause = string.Join(",", nonKeys.Select(c => $"target.{QuotedName(c)}=source.{QuotedName(c)}"));
            string insertCols = string.Join(",", allColumns);
            string insertVals = string.Join(",", allColumns.Select(c => "source." + QuotedName(c)));

            string sql = $@"
MERGE {QuotedName(table)} AS target
USING {sourceAlias}
ON {onClause}
WHEN MATCHED THEN UPDATE SET {updateClause}
WHEN NOT MATCHED THEN INSERT ({insertCols}) VALUES ({insertVals});";

            return await ExecuteNonQueryAsync(sql, paramMap);
        }
        #endregion


        public IDbTransaction BeginTransaction()
        {
            return _connection.BeginTransaction();
        }


        private async Task<int> ExecuteNonQueryAsync(string sql, IDictionary<string, object> parameters, IDbTransaction transaction = null)
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;
                if (transaction != null)
                    cmd.Transaction = transaction;

                foreach (var kv in parameters)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = "@" + kv.Key.TrimStart('@');
                    p.Value = kv.Value ?? DBNull.Value;
                    cmd.Parameters.Add(p);
                }

#if NETSTANDARD2_0
                return await Task.Run(() => cmd.ExecuteNonQuery());
#else
                return await cmd.ExecuteNonQueryAsync();
#endif
            }
        }
    }
}
