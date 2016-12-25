using System;

namespace OY.StockPriceQtyTool
{
    public class ImportLog
    {
        /// <summary>
        /// 股票编码
        /// </summary>
        public string StockCode { get; set; }
        /// <summary>
        /// 交易日期
        /// </summary>
        public DateTime DealDate { get; set; }

        /// <summary>
        /// 是否导入成功
        /// </summary>
        public bool ImportIsSuccess { get; set; }

        /// <summary>
        /// 导入操作完成时间
        /// </summary>
        public DateTime ImportDateTime { get; set; }
    }
}