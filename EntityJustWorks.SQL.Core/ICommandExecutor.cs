using System.Collections.Generic;
using System.Threading.Tasks;

namespace EntityJustWorks.SQL.Core
{
    public interface ICommandExecutor
    {
        Task<int> InsertAsync(string table, IDictionary<string, object> values);
        Task<int> UpdateAsync(string table, IDictionary<string, object> values, IDictionary<string, object> keys);
        Task<int> UpsertAsync(string table, IDictionary<string, object> values, IDictionary<string, object> keys);
        Task<int> DeleteAsync(string table, IDictionary<string, object> keys);

        // ✅ Thêm mới
        Task<int> InsertRangeAsync(string table, IEnumerable<IDictionary<string, object>> items);
        Task<int> UpsertRangeAsync(string table, IEnumerable<IDictionary<string, object>> items, IEnumerable<string> keyColumns);
    }
}
