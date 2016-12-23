using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using OY.StockPriceQtyTool.Thread;

namespace OY.StockPriceQtyTool
{
    public class CsvImporter
    {
        private readonly ConcurrentStack<KeyValuePair<FileInfo, DateTime>> _everyDayStockFileStack =
            new ConcurrentStack<KeyValuePair<FileInfo, DateTime>>();

        /// <summary>
        ///     获得待处理文件列表
        /// </summary>
        /// <param name="strBaseDir"></param>
        public void GetTodoFiles(string strBaseDir)
        {
            var mouthDir = new DirectoryInfo(strBaseDir);
            var dateDirs = mouthDir.GetDirectories();
            foreach (var dateDir in dateDirs)
            {
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

                if (curdate == null) continue;
                var stockFiles = dateDir.GetFiles();

                if (!stockFiles.Any()) continue;
                foreach (var stockFile in stockFiles)
                    _everyDayStockFileStack.Push(new KeyValuePair<FileInfo, DateTime>(stockFile, curdate.Value));
            }
        }

        /// <summary>
        ///     多线程处理文件
        /// </summary>
        public void ImportToDataBaseAsyn()
        {
            //todo 上限为线程上限，为配置项（暂定4个）
            for (var i = 0; i < 4; i++)
                WpfTask.FactoryStartNew(() =>
                {
                    #region 定义datatable

                    var insertDataTable = new DataTable();
                    insertDataTable.Columns.Add(new DataColumn("Price", typeof(decimal)));
                    insertDataTable.Columns.Add(new DataColumn("Qty", typeof(decimal)));
                    insertDataTable.Columns.Add(new DataColumn("StockCode", typeof(string)));
                    insertDataTable.Columns.Add(new DataColumn("StockIsBuyState", typeof(bool)));
                    insertDataTable.Columns.Add(new DataColumn("Date", typeof(DateTime)));

                    #endregion

                    while (true)
                        try
                        {
                            KeyValuePair<FileInfo, DateTime> result;
                            var isok = _everyDayStockFileStack.TryPeek(out result);
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
                                    newrow["Date"] = entity.Date;
                                    insertDataTable.Rows.Add(newrow);
                                }

                                if (insertDataTable.Rows.Count >= 10000) //10000条插一次库
                                {
                                    //insert数据库，并且清空insertDataTable数据
                                    BulkInsertToDataBase(insertDataTable);
                                    insertDataTable.Clear();
                                }

                                #endregion
                            }

                            else
                            {
                                return; //跳出
                            }
                        }
                        catch (Exception ex)
                        {
                            //todo 记下日志
                            Debug.WriteLine(ex.Message);
                        }
                });
        }

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
                        Date = everyDayStockFile.Value,
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
                    p.Date,
                    p.StockIsBuyState
                }
                into g
                let sumQty = g.Sum(x => x.Qty)
                select new StockOneDateInfoEntity
                {
                    Price = g.Select(f => f.Price).FirstOrDefault(),
                    Qty = sumQty,
                    StockCode = g.Select(f => f.StockCode).FirstOrDefault(),
                    Date = g.Select(f => f.Date).FirstOrDefault(),
                    StockIsBuyState = g.Select(f => f.StockIsBuyState).FirstOrDefault()
                }).ToList();

            #endregion
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