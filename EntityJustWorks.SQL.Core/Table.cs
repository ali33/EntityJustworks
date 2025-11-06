/* 
 * EntityJustWorks.SQL - C# class object to/from SQL database
 * 
 * 
 *  Full code and more available @
 *    https://csharpcodewhisperer.blogspot.com
 *    
 *  Or check for updates @
 *    https://github.com/AdamWhiteHat/EntityJustworks
 * 
 */
using System;
using System.IO;
using System.Linq;
using System.Data;
using System.Reflection;
using System.Diagnostics;
using System.Reflection.Emit;
using System.Collections.Generic;

namespace EntityJustWorks.SQL.Core
{
    public static class Table
    {
        /// <summary>
        /// Creates a DataTable from a class type's public properties and adds a new DataRow to the table for each class passed as a parameter.
        /// The DataColumns of the table will match the name and type of the public properties.
        /// </summary>
        /// <param name="classInstanceCollection">A class or array of class to fill the DataTable with.</param>
        /// <returns>A DataTable who's DataColumns match the name and type of each class T's public properties.</returns>
        public static DataTable FromClassInstanceCollection<T>(params T[] classInstanceCollection) where T : class
        {
            DataTable result = FromClass<T>();

            if (!Helper.IsValidDatatable(result, true))
            {
                return new DataTable();
            }

            if (!Helper.IsCollectionEmpty(classInstanceCollection))
            {
                foreach (T classObject in classInstanceCollection)
                {
                    FromClassInstance(result, classObject);
                }
            }
            return result;// Returns and empty DataTable with columns defined (table schema)
        }

        /// <summary>
        /// Creates a DataTable from a class type's public properties. The DataColumns of the table will match the name and type of the public properties.
        /// </summary>
        /// <typeparam name="T">The type of the class to create a DataTable from.</typeparam>
        /// <returns>A DataTable who's DataColumns match the name and type of each class T's public properties.</returns>
        public static DataTable FromClass<T>() where T : class
        {
            Type classType = typeof(T);
            string tableName = classType.Name ?? classType.UnderlyingSystemType.Name ?? "UnknownRefType";
            DataTable result = new DataTable(tableName);

            foreach (PropertyInfo property in classType.GetProperties())
            {
                DataColumn column = new DataColumn();
                column.ColumnName = property.Name;

                if (Helper.IsNullableType(property.PropertyType))
                {
                    if (property.PropertyType.IsGenericType)
                    {
                        // If Nullable<> and Generic, this is how we get the underlying Type...
                        column.DataType = property.PropertyType.GenericTypeArguments.FirstOrDefault();
                    }
                    else
                    {
                        column.DataType = property.PropertyType.UnderlyingSystemType;
                    }

                    column.AllowDBNull = true;
                }
                else
                {   // True by default, so set it false
                    column.DataType = property.PropertyType;
                    column.AllowDBNull = false;
                }

                // Add column
                result.Columns.Add(column);
            }
            return result;
        } 
        
        /// <summary>
        /// Creates a DataTable from a List of Directories. The DataColumns of the table will match the name and type of the public properties.
        /// </summary>
        /// <param name="dictionaries"></param>
        /// <returns>A DataTable who's DataColumns match the name and type of each dictionary key.</returns>
        public static DataTable FromDictionries(string tableName, IEnumerable<IDictionary<string, object>> dictionaries, bool fillData = true)
        {
            DataTable result = new DataTable(tableName);

            var sample = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            foreach (var dict in dictionaries)
            {
                foreach (var kv in dict)
                {
                    var value = kv.Value;
                    if (value == null || value == DBNull.Value)
                        continue;

                    // Nếu key chưa tồn tại thì thêm (giữ lại giá trị đầu tiên)
                    if (!sample.ContainsKey(kv.Key))
                        sample[kv.Key] = value;
                }
            }


            foreach (var kvp in sample)
            {
                DataColumn column = new DataColumn();
                column.ColumnName = kvp.Key;
                var valueType = kvp.Value.GetType();

                if (Helper.IsNullableType(valueType))
                {
                    if (valueType.IsGenericType)
                    {
                        // If Nullable<> and Generic, this is how we get the underlying Type...
                        column.DataType = valueType.GenericTypeArguments.FirstOrDefault();
                    }
                    else
                    {
                        column.DataType = valueType.UnderlyingSystemType;
                    }

                    column.AllowDBNull = true;
                }
                else
                {   // True by default, so set it false
                    column.DataType = valueType;
                    column.AllowDBNull = false;
                }

                // Add column
                result.Columns.Add(column);
            }

            if (fillData)
            {
                foreach (var dict in dictionaries)
                {
                    result = FromDictionary(result, dict, false);
                }
            }

            return result;
        }
        /// <summary>
        /// Adds a DataRow to a DataTable from the Dictionary Key Value Pairs
        /// </summary>
        /// <param name="dataTable"></param>
        /// <param name="dict"></param>
        /// <param name="checkValid"></param>
        /// <returns></returns>
        public static DataTable FromDictionary(DataTable dataTable, IDictionary<string, object> dict, bool checkValid = true)
        {
            DataRow row = dataTable.NewRow();
            foreach (var kvp in dict)
            {
                if (!checkValid || IsValidTableData(dataTable, kvp.Key))
                {
                    object value = kvp.Value;
                    row[kvp.Key] = value == null ? DBNull.Value : value;
                }
            }
            dataTable.Rows.Add(row);
            return dataTable;
        }

        /// <summary>
        /// Adds a DataRow to a DataTable from the public properties of a class.
        /// </summary>
        /// <param name="dataTable">A reference to the DataTable to insert the DataRow into.</param>
        /// <param name="classObject">The class containing the data to fill the DataRow from.</param>
        /// <returns>The DataTable in the parameters. This return instance is superflowous; </returns>
        public static DataTable FromClassInstance<T>(DataTable dataTable, T classObject) where T : class
        {
            DataRow row = dataTable.NewRow();
            foreach (PropertyInfo property in typeof(T).GetProperties())
            {
                if (IsValidTableData(dataTable, property))
                {
                    object value = property.GetValue(classObject, null);
                    row[property.Name] = value == null ? DBNull.Value : value;
                }
            }
            dataTable.Rows.Add(row);

            return dataTable;
        }

        private static bool IsValidTableData(DataTable dataTable, string columnName)
        {
            return dataTable.Columns.Contains(columnName) && dataTable.Columns[columnName] != null;
        }

        private static bool IsValidTableData(DataTable dataTable, PropertyInfo property)
        {
            return dataTable.Columns.Contains(property.Name) && dataTable.Columns[property.Name] != null;
        }

        private static bool IsValidObjectData(PropertyInfo Property, List<string> ColumnNames, DataRow Row)
        {
            if (Property == null || // Null check
                !Property.CanWrite ||  // Make sure property isn't read only
                !ColumnNames.Contains(Property.Name) || // If property is a column name
                Row[Property.Name] == DBNull.Value) // Don't copy over DBNull
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Fill List of dictionaries from rows of a DataTable where the name of the property matches the column name from that DataTable.
        /// </summary>
        /// <param name="dataTable"></param>
        /// <returns></returns>
        public static IList<IDictionary<string, object>> ToDictionaries(DataTable dataTable)
        {
            if (!Helper.IsValidDatatable(dataTable))
                return new List<IDictionary<string, object>>();

            IList<IDictionary<string, object>> result = new List<IDictionary<string, object>>(dataTable.Rows.Count);
            var columnNames = dataTable.Columns.Cast<DataColumn>().Select(column => column.ColumnName);
            foreach (DataRow row in dataTable.Rows)
            {
                var dict = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var column in columnNames)
                {
                    if (row[column] == DBNull.Value)
                        continue;
                    dict[column] = row[column];
                }
            }
            return result;
        }

        /// <summary>
        /// Fills properties of a class from a row of a DataTable where the name of the property matches the column name from that DataTable.
        /// It does this for each row in the DataTable, returning a List of classes.
        /// </summary>
        /// <typeparam name="T">The class type that is to be returned.</typeparam>
        /// <param name="dataTable">DataTable to fill from.</param>
        /// <returns>A list of ClassType with its properties set to the data from the matching columns from the DataTable.</returns>
        public static IList<T> ToClassInstanceCollection<T>(DataTable dataTable) where T : class, new()
        {
            if (!Helper.IsValidDatatable(dataTable))
                return new List<T>();

            Type classType = typeof(T);
            IList<PropertyInfo> propertyList = classType.GetProperties();

            // Parameter class has no public properties.
            if (propertyList.Count == 0)
                return new List<T>();

            List<string> columnNames = dataTable.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToList();

            List<T> result = new List<T>();
            try
            {
                foreach (DataRow row in dataTable.Rows)
                {
                    T classObject = new T();
                    foreach (PropertyInfo property in propertyList)
                    {
                        if (!IsValidObjectData(property, columnNames, row))
                            continue;

                        object propertyValue = Convert.ChangeType(
                                row[property.Name],
                                property.PropertyType
                            );

                        property.SetValue(classObject, propertyValue, null);
                    }
                    result.Add(classObject);
                }
                return result;
            }
            catch
            {
                return new List<T>();
            }
        }

        /// <summary>
        /// Executes an SQL query and returns the results as a DataTable.
        /// </summary>
        /// <param name="connectionString">The SQL connection string.</param>
        /// <param name="formatString_Query">A SQL command that will be passed to string.Format().</param>
        /// <param name="formatString_Parameters">The parameters for string.Format().</param>
        /// <returns>The results of the query as a DataTable.</returns>
        public static DataTable FromQuery(string connectionString, string formatString_Query, params object[] formatString_Parameters)
        {
            return DatabaseQuery.ToDataTable(connectionString, formatString_Query, formatString_Parameters);
        }

        /// <summary> 
        /// Generates the C# class code from a DataTable.
        /// </summary>
        /// <returns>The C# class code as a string.</returns>
        public static FileInfo ToCSharpCode(DataTable dataTable, string customSavePath = "{Namespace}.{Class}.cs")
        {
            string className = dataTable.TableName;
            if (string.IsNullOrWhiteSpace(className))
            {
                string randomName = Path.GetFileNameWithoutExtension(Path.GetRandomFileName());
                className = string.Format("Unnamed_{0}", randomName.Substring(0, randomName.Length / 2));
            }

            // Find namespace name, create CodeNamespace
            string namespaceName = new StackFrame(2).GetMethod().DeclaringType.Namespace;// "EntityJustWorks.AutoGeneratedClassObject";

            string filename = null;
            if (!string.IsNullOrWhiteSpace(customSavePath))
            {
                filename = customSavePath.Replace("{Namespace}", namespaceName).Replace("{Class}", className);// Create filename
            }

            return Code.CreateCode(dataTable, filename); // Generate code, save it to file and return the FileInfo
        }

        /// <summary> 
        /// Emits a C# Assembly with classes whos public properties match the columns of the DataTable.
        /// </summary>
        /// <returns>The C# Type for the emitted class</returns>


        public static Type ToAssembly(DataTable dataTable)
        {
            if (dataTable == null)
                throw new ArgumentNullException(nameof(dataTable));

            string typeName = dataTable.TableName ?? "DynamicType";
            string moduleName = $"{typeName}Module";
            string assemblyName = $"{typeName}Assembly";

            // Trong .NET Standard 2.0: chỉ hỗ trợ Run, không có RunAndSave
            AssemblyName asmName = new AssemblyName(assemblyName);
            AssemblyBuilder assemblyBuilder =
#if NETSTANDARD2_0
            AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
#else
                AppDomain.CurrentDomain.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.RunAndSave);
#endif

            ModuleBuilder moduleBuilder =
#if NETSTANDARD2_0
            assemblyBuilder.DefineDynamicModule(moduleName);
#else
                assemblyBuilder.DefineDynamicModule(moduleName, $"{moduleName}.mod", true);
#endif

            TypeAttributes typeAttributes =
                TypeAttributes.Class |
                TypeAttributes.Sealed |
                TypeAttributes.Public |
                TypeAttributes.AnsiClass |
                TypeAttributes.AutoClass;

            TypeBuilder typeBuilder = moduleBuilder.DefineType(typeName, typeAttributes);

            foreach (DataColumn column in dataTable.Columns)
            {
                string propertyName = column.ColumnName;
                Type propertyType = column.DataType;

                FieldBuilder fieldPropBacker = typeBuilder.DefineField(
                    "_" + propertyName.ToLowerInvariant(),
                    propertyType,
                    FieldAttributes.Private
                );

                PropertyBuilder propertyBuilder = typeBuilder.DefineProperty(
                    propertyName,
                    PropertyAttributes.HasDefault,
                    propertyType,
                    null
                );

                MethodAttributes accessorAttributes =
                    MethodAttributes.Public |
                    MethodAttributes.SpecialName |
                    MethodAttributes.HideBySig;

                // get
                MethodBuilder getAccessor = typeBuilder.DefineMethod(
                    "get_" + propertyName,
                    accessorAttributes,
                    propertyType,
                    Type.EmptyTypes
                );

                ILGenerator getIL = getAccessor.GetILGenerator();
                getIL.Emit(OpCodes.Ldarg_0);
                getIL.Emit(OpCodes.Ldfld, fieldPropBacker);
                getIL.Emit(OpCodes.Ret);

                // set
                MethodBuilder setAccessor = typeBuilder.DefineMethod(
                    "set_" + propertyName,
                    accessorAttributes,
                    null,
                    new[] { propertyType }
                );

                ILGenerator setIL = setAccessor.GetILGenerator();
                setIL.Emit(OpCodes.Ldarg_0);
                setIL.Emit(OpCodes.Ldarg_1);
                setIL.Emit(OpCodes.Stfld, fieldPropBacker);
                setIL.Emit(OpCodes.Ret);

                propertyBuilder.SetGetMethod(getAccessor);
                propertyBuilder.SetSetMethod(setAccessor);
            }

            // .NET Standard 2.0 chỉ hỗ trợ CreateTypeInfo().AsType()
#if NETSTANDARD2_0
            return typeBuilder.CreateTypeInfo().AsType();
#else
            Type result = typeBuilder.CreateType();
            assemblyBuilder.Save(assemblyName + ".dll");
            return result;
#endif
        }
    }

}