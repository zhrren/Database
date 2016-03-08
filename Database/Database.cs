using System;
using System.Data;
using System.Data.Common;
using System.Configuration;
using System.Collections.Generic;
using System.Text;

namespace Mark.Database
{
    public class Database
    {
        private ProviderFactory providerFactory;

        public int CommandTimeout { get; set; }

        public Database()
        {
            string connectionStringName = GetType().Name;
            providerFactory = ProviderFactory.GetProvider(connectionStringName);
            CommandTimeout = 30;
        }

        public Database(string connectionStringName)
        {
            providerFactory = ProviderFactory.GetProvider(connectionStringName);
            CommandTimeout = 30;
        }

        public Database(string connectionString, string providerName)
        {
            providerFactory = ProviderFactory.GetProvider(connectionString, providerName);
            CommandTimeout = 30;
        }

        public ProviderFactory ProviderFactory
        {
            get { return providerFactory; }
        }


        #region CreateConnection,CreateCommand,CreateAdapter,CreateParameter

        public virtual DbConnection CreateConnection()
        {
            return ProviderFactory.CreateConnection();
        }
        /// <summary>
        /// 创建执行命令对象
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="cmdType"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public virtual DbCommand CreateCommand(string sql, CommandType cmdType, params IDataParameter[] parameters)
        {
            DbCommand command = providerFactory.CreateCommand();
            command.Connection = CreateConnection();
            command.CommandText = sql;
            command.CommandType = cmdType;
            command.CommandTimeout = CommandTimeout;

            if (parameters != null)
                foreach (IDataParameter param in parameters)
                {
                    command.Parameters.Add(param);
                }
            return command;
        }

        /// <summary>
        /// 创建数据适配器
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <param name="parameters">参数</param>
        /// <returns>数据适配器实例</returns>
        public virtual DbDataAdapter CreateAdapter(string sql, CommandType cmdtype, params IDataParameter[] parameters)
        {

            DbCommand command = providerFactory.CreateCommand();
            command.Connection = CreateConnection();
            command.CommandText = sql;
            command.CommandType = cmdtype;
            command.CommandTimeout = CommandTimeout;
            if (parameters != null)
                foreach (IDataParameter _param in parameters)
                {
                    command.Parameters.Add(_param);
                }
            DbDataAdapter da = providerFactory.CreateDataAdapter();
            da.SelectCommand = command;

            return da;
        }

        public virtual IDataParameter CreateParameter(string field, object value)
        {
            IDataParameter p = providerFactory.CreateParameter();
            p.ParameterName = field;
            p.Value = value;
            return p;
        }
        /// <summary>
        /// 创建参数
        /// </summary>
        /// <param name="field">参数字段</param>
        /// <param name="dbtype">参数类型</param>
        /// <param name="value">参数值</param>
        /// <returns></returns>
        public virtual IDataParameter CreateParameter(string field, object value, DbType dbtype)
        {
            IDataParameter p = providerFactory.CreateParameter();
            p.ParameterName = field;
            p.Value = value;
            p.DbType = dbtype;
            return p;
        }

        #endregion 数据库操作对象

        #region ExecuteNonQuery

        public void ExecuteNonQuery(Command command)
        {
            ExecuteNonQuery(command.CommandText, command.CommandType, command.Parameters);
        }

        /// <summary>
        /// 执行非查询语句,并返回受影响的记录行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>受影响记录行数</returns>
        public void ExecuteNonQuery(string sql)
        {
            ExecuteNonQuery(sql, CommandType.Text, (IDataParameter[])null);
        }

        /// <summary>
        /// 执行非查询语句,并返回受影响的记录行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <returns>受影响记录行数</returns>
        public void ExecuteNonQuery(string sql, CommandType cmdtype)
        {
            ExecuteNonQuery(sql, CommandType.Text, (IDataParameter[])null);
        }

        /// <summary>
        /// 执行非查询语句,并返回受影响的记录行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parameters">参数</param>
        /// <returns>受影响记录行数</returns>
        public void ExecuteNonQuery(string sql, params IDataParameter[] parameters)
        {
            ExecuteNonQuery(sql, CommandType.Text, parameters);
        }

        /// <summary>
        /// 执行非查询语句,并返回受影响的记录行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <param name="parameters">参数</param>
        /// <returns>受影响记录行数</returns>
        public void ExecuteNonQuery(string sql, CommandType cmdtype, params IDataParameter[] parameters)
        {
            DbCommand command = providerFactory.CreateCommand();
            command.CommandText = sql;
            command.CommandType = cmdtype;
            command.CommandTimeout = CommandTimeout;

            if (parameters != null)
                foreach (IDataParameter param in parameters)
                {
                    command.Parameters.Add(param);
                }


            if (null != Transaction.Current)
            {
                command.Connection = Transaction.Current.DbTransactionWrapper.DbTransaction.Connection;
                command.Transaction = Transaction.Current.DbTransactionWrapper.DbTransaction;
            }
            else
            {
                command.Connection = CreateConnection();
                command.Connection.Open();
            }

            try
            {
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new CommandException(ex, sql, cmdtype, parameters);
            }
            finally
            {
                if (null == Transaction.Current)
                    command.Connection.Dispose();
            }
        }

        #endregion

        #region ExecuteScalar

        public object ExecuteScalar(Command command)
        {
            return ExecuteScalar(command.CommandText, command.CommandType, command.Parameters);
        }

        /// <summary>
        /// 执行非查询语句,并返回首行首列的值
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>Object</returns>
        public object ExecuteScalar(string sql)
        {
            return ExecuteScalar(sql, CommandType.Text, (IDataParameter[])null);
        }
        /// <summary>
        /// 执行非查询语句,并返回首行首列的值
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <returns>Object</returns>
        public object ExecuteScalar(string sql, CommandType cmdtype)
        {
            return ExecuteScalar(sql, CommandType.Text, (IDataParameter[])null);
        }
        /// <summary>
        /// 执行非查询语句,并返回首行首列的值
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parameters">参数</param>
        /// <returns>Object</returns>
        public object ExecuteScalar(string sql, params IDataParameter[] parameters)
        {
            return ExecuteScalar(sql, CommandType.Text, (IDataParameter[])parameters);
        }
        /// <summary>
        /// 执行非查询语句,并返回首行首列的值
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <param name="parameters">参数</param>
        /// <returns>Object</returns>
        public object ExecuteScalar(string sql, CommandType cmdtype, params IDataParameter[] parameters)
        {
            object result = null;
            DbCommand command = CreateCommand(sql, cmdtype, parameters);
            try
            {
                command.Connection.Open();
                result = command.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new CommandException(ex, sql, cmdtype, parameters);
            }
            finally
            {
                command.Connection.Dispose();
            }
            return result;
        }
        #endregion

        #region ExecuteReader

        public DbDataReader ExecuteReader(Command command)
        {
            return ExecuteReader(command.CommandText, command.CommandType, command.Parameters);
        }

        /// <summary>
        /// 执行查询，并以DataReader返回结果集
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>IDataReader</returns>
        public DbDataReader ExecuteReader(string sql)
        {
            return ExecuteReader(sql, CommandType.Text, (IDataParameter[])null);
        }
        /// <summary>
        /// 执行查询，并以DataReader返回结果集
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <returns>IDataReader</returns>
        public DbDataReader ExecuteReader(string sql, CommandType cmdtype)
        {
            return ExecuteReader(sql, CommandType.Text, (IDataParameter[])null);
        }
        /// <summary>
        /// 执行查询，并以DataReader返回结果集
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parameters">参数</param>
        /// <returns>IDataReader</returns>
        public DbDataReader ExecuteReader(string sql, params IDataParameter[] parameters)
        {
            return ExecuteReader(sql, CommandType.Text, parameters);
        }
        /// <summary>
        /// 执行查询，并以DataReader返回结果集
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <param name="parameters">参数</param>
        /// <returns>IDataReader</returns>
        public DbDataReader ExecuteReader(string sql, CommandType cmdtype, params IDataParameter[] parameters)
        {
            DbDataReader result;
            DbCommand command = CreateCommand(sql, cmdtype, parameters);
            try
            {
                command.Connection.Open();
                result = command.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception ex)
            {
                throw new CommandException(ex, sql, cmdtype, parameters);
            }
            finally
            {

            }
            return result;
        }
        #endregion

        #region ExecuteDataSet

        public DataSet ExecuteDataSet(Command command)
        {
            return ExecuteDataSet(command.CommandText, command.CommandType, command.Parameters);
        }

        /// <summary>
        /// 执行查询，并以DataSet返回结果集
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>DataSet</returns>
        public DataSet ExecuteDataSet(string sql)
        {
            return ExecuteDataSet(sql, CommandType.Text, (IDataParameter[])null);
        }
        /// <summary>
        /// 执行查询，并以DataSet返回结果集
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <returns>DataSet</returns>
        public DataSet ExecuteDataSet(string sql, CommandType cmdtype)
        {
            return ExecuteDataSet(sql, CommandType.Text, (IDataParameter[])null);
        }
        /// <summary>
        /// 执行查询，并以DataSet返回结果集
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parameters">参数</param>
        /// <returns>DataSet</returns>
        public DataSet ExecuteDataSet(string sql, params IDataParameter[] parameters)
        {
            return ExecuteDataSet(sql, CommandType.Text, parameters);
        }
        /// <summary>
        /// 执行查询，并以DataSet返回结果集
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <param name="parameters">参数</param>
        /// <returns>DataSet</returns>
        public DataSet ExecuteDataSet(string sql, CommandType cmdtype, params IDataParameter[] parameters)
        {
            DataSet result = new DataSet();
            DbDataAdapter dataAdapter = CreateAdapter(sql, cmdtype, parameters);
            try
            {
                dataAdapter.Fill(result);
            }
            catch (Exception ex)
            {
                throw new CommandException(ex, sql, cmdtype, parameters);
            }
            finally
            {
                dataAdapter.Dispose();
            }
            return result;
        }
        #endregion

        #region ExecuteDataRow

        public DataRow ExecuteDataRow(Command command)
        {
            return ExecuteDataRow(command.CommandText, command.CommandType, command.Parameters);
        }

        /// <summary>
        /// 执行查询，并返回DataRow
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>DataRow</returns>
        public DataRow ExecuteDataRow(string sql)
        {
            return ExecuteDataRow(sql, CommandType.Text, (IDataParameter[])null);
        }
        /// <summary>
        /// 执行查询，并返回DataRow
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <returns>DataRow</returns>
        public DataRow ExecuteDataRow(string sql, CommandType cmdtype)
        {
            return ExecuteDataRow(sql, CommandType.Text, (IDataParameter[])null);
        }
        /// <summary>
        /// 执行查询，并返回DataRow
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parameters">参数</param>
        /// <returns>DataRow</returns>
        public DataRow ExecuteDataRow(string sql, params IDataParameter[] parameters)
        {
            return ExecuteDataRow(sql, CommandType.Text, parameters);
        }
        /// <summary>
        /// 执行查询，并返回DataRow
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cmdtype">命令类型</param>
        /// <param name="parameters">参数</param>
        /// <returns>DataRow</returns>
        public DataRow ExecuteDataRow(string sql, CommandType cmdtype, params IDataParameter[] parameters)
        {
            DataSet result = new DataSet();
            DbDataAdapter dataAdapter = CreateAdapter(sql, cmdtype, parameters);
            try
            {
                dataAdapter.Fill(result);
            }
            catch (Exception ex)
            {
                throw new CommandException(ex, sql, cmdtype, parameters);
            }
            finally
            {
                dataAdapter.Dispose();
            }

            if (result.Tables[0].Rows.Count > 0)
                return result.Tables[0].Rows[0];
            else
                return (DataRow)null;
        }
        #endregion

    }

    public class ProviderFactory
    {
        private static Dictionary<string, ProviderFactory> providerFactoryCache = new Dictionary<string, ProviderFactory>();

        public static ProviderFactory GetProvider(string connectionStringname)
        {
            lock (providerFactoryCache)
            {
                ProviderFactory entry = null;
                if (!providerFactoryCache.TryGetValue(connectionStringname, out entry))
                {
                    var conn = ConfigurationManager.ConnectionStrings[connectionStringname];
                    entry = new ProviderFactory(conn.ConnectionString, conn.ProviderName);
                    providerFactoryCache.Add(connectionStringname, entry);
                }
                return entry;
            }
        }

        public static ProviderFactory GetProvider(string connectionString, string providerName)
        {
            lock (providerFactoryCache)
            {
                ProviderFactory entry = null;
                if (!providerFactoryCache.TryGetValue(connectionString, out entry))
                {
                    entry = new ProviderFactory(connectionString, providerName);
                    providerFactoryCache.Add(connectionString, entry);
                }
                return entry;
            }
        }

        private DbProviderFactory provider;

        private ProviderFactory(string connectionString, string providerInvariantName)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(providerInvariantName))
                throw new ArgumentNullException("providerInvariantName");

            ConnectionString = connectionString;
            ProviderName = providerInvariantName;

            provider = DbProviderFactories.GetFactory(providerInvariantName);
        }

        public string ProviderName { get; private set; }

        public string ConnectionString { get; private set; }

        public virtual DbCommand CreateCommand()
        {
            return provider.CreateCommand();
        }
        public virtual DbCommandBuilder CreateCommandBuilder()
        {
            return provider.CreateCommandBuilder();
        }
        public virtual DbConnection CreateConnection()
        {
            var conn = provider.CreateConnection();
            conn.ConnectionString = ConnectionString;
            return conn;
        }
        public virtual DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return provider.CreateConnectionStringBuilder();
        }
        public virtual DbDataAdapter CreateDataAdapter()
        {
            return provider.CreateDataAdapter();
        }
        public virtual DbDataSourceEnumerator CreateDataSourceEnumerator()
        {
            return provider.CreateDataSourceEnumerator();
        }
        public virtual DbParameter CreateParameter()
        {
            return provider.CreateParameter();
        }
    }

    public struct Command
    {
        private CommandType commandType;
        private string commandText;
        private IDataParameter[] parameters;

        public Command(string commandText, IDataParameter[] parameters)
            : this(commandText, CommandType.Text, parameters)
        {
        }

        public Command(string commandText, CommandType commandType, IDataParameter[] parameters)
        {
            this.commandText = commandText;
            this.commandType = commandType;
            this.parameters = parameters;
        }

        public CommandType CommandType
        {
            get { return commandType; }
            set { commandType = value; }
        }

        public string CommandText
        {
            get { return commandText; }
            set { commandText = value; }
        }

        public IDataParameter[] Parameters
        {
            get { return parameters; }
            set { parameters = value; }
        }
    }

    public class CommandException : ApplicationException
    {
        string message;
        string commandText;
        CommandType commandType;
        IDataParameter[] parameters;

        public CommandException(Exception innerException,
            string commandText,
            CommandType cmdtype,
            IDataParameter[] parameters)
            : base("sql error", innerException)
        {
            this.commandText = commandText;
            this.commandType = cmdtype;
            this.parameters = parameters;
        }

        public string CommandText
        {
            get { return commandText; }
        }

        public CommandType CommandType
        {
            get { return commandType; }
        }

        public IDataParameter[] Parameters
        {
            get { return parameters; }
        }

        public override string Message
        {
            get
            {
                FormatMessage();
                return message;
            }
        }

        private void FormatMessage()
        {
            if (message == null)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("(");
                sb.Append(CommandType);
                sb.Append(")");
                sb.Append(CommandText);
                if (Parameters != null)
                {
                    sb.AppendLine();

                    foreach (var p in Parameters)
                    {
                        sb.Append("(");
                        sb.Append(p.DbType);
                        sb.Append(",");
                        var dbp = p as IDbDataParameter;
                        if (dbp != null)
                        {
                            sb.Append(dbp.Size);
                            sb.Append(",");
                        }
                        sb.Append(p.Direction);
                        sb.Append(")");
                        sb.Append(p.ParameterName);
                        sb.Append("=");
                        sb.Append(p.Value);
                        sb.AppendLine();
                    }
                }
                sb.AppendLine();
                message = sb.ToString();
            }
        }


    }

    public class DbTransactionWrapper : IDisposable
    {
        public DbTransactionWrapper(DbTransaction transaction)
        {
            this.DbTransaction = transaction;
        }
        public DbTransaction DbTransaction { get; private set; }
        public bool IsRollBack { get; set; }
        public void Rollback()
        {
            if (!this.IsRollBack)
            {
                this.DbTransaction.Rollback();
            }
        }
        public void Commit()
        {
            this.DbTransaction.Commit();
        }
        public void Dispose()
        {
            this.DbTransaction.Dispose();
        }
    }

    public abstract class Transaction : IDisposable
    {
        [ThreadStatic]
        private static Transaction current;

        public bool Completed { get; private set; }
        public DbTransactionWrapper DbTransactionWrapper { get; protected set; }
        protected Transaction() { }
        public void Rollback()
        {
            this.DbTransactionWrapper.Rollback();
        }
        public DependentTransaction DependentClone()
        {
            return new DependentTransaction(this);
        }
        public void Dispose()
        {
            this.DbTransactionWrapper.Dispose();
        }
        public static Transaction Current
        {
            get { return current; }
            set { current = value; }
        }
    }

    public class CommittableTransaction : Transaction
    {
        public CommittableTransaction(DbTransaction dbTransaction)
        {
            this.DbTransactionWrapper = new DbTransactionWrapper(dbTransaction);
        }
        public void Commit()
        {
            this.DbTransactionWrapper.Commit();
        }
    }
    public class DependentTransaction : Transaction
    {
        public Transaction InnerTransaction { get; private set; }
        internal DependentTransaction(Transaction innerTransaction)
        {
            this.InnerTransaction = innerTransaction;
            this.DbTransactionWrapper = this.InnerTransaction.DbTransactionWrapper;
        }
    }
    public class TransactionScope : IDisposable
    {
        private Transaction transaction = Transaction.Current;
        public bool Completed { get; private set; }

        public TransactionScope(DbConnection connection, IsolationLevel isolationLevel = IsolationLevel.Unspecified)
        {
            if (null == transaction)
            {
                if (connection.State == ConnectionState.Closed)
                    connection.Open();
                DbTransaction dbTransaction = connection.BeginTransaction(isolationLevel);
                Transaction.Current = new CommittableTransaction(dbTransaction);
            }
            else
            {
                Transaction.Current = transaction.DependentClone();
            }
        }

        public void Complete()
        {
            this.Completed = true;
        }
        public void Dispose()
        {
            Transaction current = Transaction.Current;
            Transaction.Current = transaction;
            if (!this.Completed)
            {
                current.Rollback();
            }
            CommittableTransaction committableTransaction = current as CommittableTransaction;
            if (null != committableTransaction)
            {
                if (this.Completed)
                {
                    committableTransaction.Commit();
                }
                committableTransaction.Dispose();
            }
        }
    }
}