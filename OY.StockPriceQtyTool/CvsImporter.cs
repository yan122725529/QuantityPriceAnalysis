using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows;
using OY.StockPriceQtyTool.Thread;

namespace OY.StockPriceQtyTool
{
    public class CsvImporter
    {
        private readonly ConcurrentStack<KeyValuePair<FileInfo, DateTime>> _everyDayStockFileStack =
            new ConcurrentStack<KeyValuePair<FileInfo, DateTime>>();

        private readonly IList<ImportLog> _importLogs = new List<ImportLog>();

        private readonly ConcurrentStack<ImportLog> _logJobStack =
            new ConcurrentStack<ImportLog>();

        /// <summary>
        ///     增加新的股票代码信息s
        /// </summary>
        private void AddNewStockInfo(IEnumerable<StockInfoEntity> stockInfoEntities)
        {
            //todo 从数据库获取数据
            IList<StockInfoEntity> stockInfosinDb = new List<StockInfoEntity>();
            stockInfosinDb.Add(new StockInfoEntity());

            foreach (var entity in stockInfoEntities)
                if (stockInfosinDb.Any(x => x.StockCode == entity.StockCode))
                {
                    //todo 存股票信息数据库
                }
        }

        /// <summary>
        ///     检查是否已经有导入的数据
        /// </summary>
        /// <param name="stockCode"></param>
        /// <param name="dealDate"></param>
        /// <returns></returns>
        private bool CheckDataIsExists(string stockCode, DateTime dealDate)
        {
            return _importLogs.Any(x => x.ImportIsSuccess && (x.StockCode == stockCode) && (x.DealDate == dealDate));
        }

        /// <summary>
        ///     获得待处理文件列表
        /// </summary>
        /// <param name="strBaseDir"></param>
        public void GetTodoFiles(string strBaseDir)
        {
            var mouthDir = new DirectoryInfo(strBaseDir);
            var dateDirs = mouthDir.GetDirectories();
            var exsitsInfos = new List<ImportLog>(); //缓存在LOG中标记已经存在的数据
            foreach (var dateDir in dateDirs)
            {
                #region 从文件夹名称获取时间信息

                DateTime? curdate = null;
                try
                {
                    var year = dateDir.Name.Substring(0, 4).ToInt32();
                    var mouth = dateDir.Name.Substring(4, 2).ToInt32();
                    var day = dateDir.Name.Substring(6, 2).ToInt32();
                    curdate = new DateTime(year, mouth, day);
                }
                catch (Exception)
                {
                    //TODO 记下日志，文件夹名称不能转换成时间
                }

                #endregion

                #region 从文件名获取文件信息

                if (curdate == null) continue;
                var stockFiles = dateDir.GetFiles();

                if (!stockFiles.Any()) continue;
                var everyDayStockFilelist = new List<KeyValuePair<FileInfo, DateTime>>();
                foreach (var stockFile in stockFiles)
                {
                    everyDayStockFilelist.Add(new KeyValuePair<FileInfo, DateTime>(stockFile, curdate.Value));
                    if (CheckDataIsExists(stockFile.Name.Replace(".csv", string.Empty), curdate.Value))
                        _importLogs.Add(new ImportLog
                        {
                            StockCode = stockFile.Name.Replace(".csv", string.Empty),
                            DealDate = curdate.Value
                        });
                }

                #endregion

                #region 处理本次所有的股票信息

                AddNewStockInfo(
                    everyDayStockFilelist.Select(
                        x => new StockInfoEntity {StockCode = x.Key.Name.Replace(".csv", string.Empty)}));

                #endregion

                #region 检查数据是否已存在，并进行处理

                var forceimport = true;
                //todo 从数据库获取_importLogs
                if (exsitsInfos.Any())
                {
                    var firstOrDefault = exsitsInfos.FirstOrDefault();
                    if (firstOrDefault != null)
                        forceimport = MessageBox.Show(Application.Current.MainWindow,
                                          $@"发现股票编码为{firstOrDefault.StockCode},导入日期为{firstOrDefault.DealDate:yyyy-MM-dd} 等 {exsitsInfos
                                              .Count} 条数据存在导入记录，是否强制覆盖导入？", "警告",
                                          MessageBoxButton.YesNo, MessageBoxImage.Warning, MessageBoxResult.No) ==
                                      MessageBoxResult.Yes;
                }

                #endregion

                //检查是否已经有导入的数据，并询问是否需要重新到处

                #region 处理工作队列

                if (forceimport)
                {
                    foreach (var info in exsitsInfos)
                    {
                        //todo 删除数据已存在数据
                    }

                    foreach (var job in everyDayStockFilelist)
                        _everyDayStockFileStack.Push(job);
                }

                else
                {
                    foreach (var job in everyDayStockFilelist)
                        if (!exsitsInfos.Any(
                            x =>
                                (x.StockCode == job.Key.Name.Replace(".csv", string.Empty)) &&
                                (x.DealDate == job.Value)))
                            _everyDayStockFileStack.Push(job);
                }

                #endregion
            }
        }

        private readonly object _countLock=new object();

        /// <summary>
        ///     多线程处理文件
        /// </summary>
        public void ImportToDataBaseAsyn()
        {
          
            var donetaskcount = 0; //完成的任务个数
            for (var i = 0; i < 4; i++)
                WpfTask.FactoryStartNew(() =>
                {
                    #region 定义datatable

                    var insertDataTable = new DataTable();
                    insertDataTable.Columns.Add(new DataColumn("Price", typeof(decimal)));
                    insertDataTable.Columns.Add(new DataColumn("Qty", typeof(decimal)));
                    insertDataTable.Columns.Add(new DataColumn("StockCode", typeof(string)));
                    insertDataTable.Columns.Add(new DataColumn("StockIsBuyState", typeof(bool)));
                    insertDataTable.Columns.Add(new DataColumn("DealDate", typeof(DateTime)));
                    IList<Tuple<string, DateTime>> doneList = new List<Tuple<string, DateTime>>();

                    #endregion

                    while (true)
                        try
                        {
                            KeyValuePair<FileInfo, DateTime> result;
                            var isok = _everyDayStockFileStack.TryPop(out result);
                            if (isok)
                            {
                                var list = OpenCsvToStockOneDateInfoEntityList(result);

                                #region 生成Datatable

                                foreach (var entity in list)
                                {
                                    var newrow = insertDataTable.NewRow();
                                    newrow["Price"] = entity.Price;
                                    newrow["Qty"] = entity.Qty;
                                    newrow["StockCode"] = entity.StockCode;
                                    newrow["StockIsBuyState"] = entity.StockIsBuyState;
                                    newrow["DealDate"] = entity.DealDate;
                                    insertDataTable.Rows.Add(newrow);
                                }
                                doneList.Add(new Tuple<string, DateTime>(result.Key.Name.Replace(".csv", string.Empty),
                                    result.Value));
                                if (insertDataTable.Rows.Count < 10000) continue;

                                #region insert数据库，并且清空insertDataTable数据

                                try
                                {
                                    BulkInsertToDataBase(insertDataTable);
                                    insertDataTable.Clear();
                                }
                                catch (Exception)
                                {
                                    insertDataTable.Clear();
                                    throw;
                                }

                         

                                #endregion

                                #region 日志处理

                                foreach (var job in doneList)
                                    _logJobStack.Push(new ImportLog
                                    {
                                        StockCode = job.Item1,
                                        DealDate = job.Item2,
                                        ImportDateTime = DateTime.Now,
                                        ImportIsSuccess = true
                                    });


                                doneList.Clear();

                                #endregion

                                #endregion
                            }

                            else
                            {
                                if (insertDataTable.Rows.Count <= 0) return;
                                try
                                {
                                    BulkInsertToDataBase(insertDataTable);
                                }
                                catch (Exception)
                                {
                                    insertDataTable.Clear();
                                    throw;
                                }
                                return; //跳出
                            }
                        }
                        catch (Exception ex)
                        {
                            //todo 记下日志
                            Debug.WriteLine(ex.Message);

                            foreach (var job in doneList)
                                _logJobStack.Push(new ImportLog
                                {
                                    StockCode = job.Item1,
                                    DealDate = job.Item2,
                                    ImportDateTime = DateTime.Now,
                                    ImportIsSuccess = false
                                });

                            _logJobStack.Clear();
                        }
                }).ContinueWith(task =>
                {
                    lock (_countLock)
                    {
                        donetaskcount = donetaskcount + 1;
                    }
                    if (donetaskcount >= 4)
                    {
                        AfterImportAction?.Invoke();
                        Debug.WriteLine("已完成");
                    }


                }).ContinueWith(task => { InsertLogAsyn(); });
        }

        public Action AfterImportAction { get; set; }

        /// <summary>
        ///     将CSV文件的数据读取，并且输出实体list
        /// </summary>
        /// <returns>返回读取了CSV数据的DataTable</returns>
        public IList<StockOneDateInfoEntity> OpenCsvToStockOneDateInfoEntityList(
            KeyValuePair<FileInfo, DateTime> everyDayStockFile)
        {
            var csvList = new List<StockOneDateInfoEntity>();

            #region 读取原始数据

            using (var csvFileStream = new FileStream(everyDayStockFile.Key.FullName, FileMode.Open, FileAccess.Read))
            {
                var csvStreamReader = new StreamReader(csvFileStream, Encoding.UTF8);
                //记录每次读取的一行记录
                string strLine;
                //记录每行记录中的各字段内容


                //逐行读取CSV中的数据
                while ((strLine = csvStreamReader.ReadLine()) != null)
                {
                    var aryLine = strLine.Split(',');
                    csvList.Add(new StockOneDateInfoEntity
                    {
                        Price = aryLine[1].ToDecimalOrDefault(0),
                        StockIsBuyState = aryLine[2].ToLower() == "b",
                        Qty = aryLine[3].ToDecimalOrDefault(0),
                        DealDate = everyDayStockFile.Value,
                        StockCode = everyDayStockFile.Key.Name.Replace(".csv", string.Empty)
                    });
                }

                csvFileStream.Close();
                csvStreamReader.Close();
            }

            #endregion

            #region 数据分组计算

            return (from p in csvList.ToArray()
                group p by new
                {
                    p.Price,
                    p.StockCode,
                    Date = p.DealDate,
                    p.StockIsBuyState
                }
                into g
                let sumQty = g.Sum(x => x.Qty)
                select new StockOneDateInfoEntity
                {
                    Price = g.Select(f => f.Price).FirstOrDefault(),
                    Qty = sumQty,
                    StockCode = g.Select(f => f.StockCode).FirstOrDefault(),
                    DealDate = g.Select(f => f.DealDate).FirstOrDefault(),
                    StockIsBuyState = g.Select(f => f.StockIsBuyState).FirstOrDefault()
                }).ToList();

            #endregion
        }

        /// <summary>
        ///     异步写log
        /// </summary>
        public void InsertLogAsyn()
        {
            WpfTask.FactoryStartNew(() =>
            {
                while (true)
                {
                    ImportLog logjob;
                    var result = _logJobStack.TryPop(out logjob);
                    if (!result) return;
                    //logjob 存数据库
                }
            });
        }

        public void BulkInsertToDataBase(DataTable insertDataTable)
        {
            Debug.WriteLine("insertDataTableCount:" + insertDataTable.Rows.Count);


            ////映射表的列和Datatable的列
            //var copy = new SqlBulkCopy(conn, SqlBulkCopyOptions.CheckConstraints, tran)
            //{
            //    DestinationTableName = string.Format(@"#{0}", recondPar.Key)
            //};

            //foreach (DataColumn dc in resultDataTable.Columns)
            //{
            //    copy.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
            //}
            //copy.WriteToServer(dt);
        }
    }
}