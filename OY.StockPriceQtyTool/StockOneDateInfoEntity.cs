using System;

namespace OY.StockPriceQtyTool
{
    public class StockOneDateInfoEntity
    {

        /// <summary>
        ///价格
        /// </summary>
        public decimal Price { get; set; }
        /// <summary>
        /// 数量
        /// </summary>
        public decimal Qty { get; set; }
        /// <summary>
        /// 股票编码
        /// </summary>
        public string StockCode { get; set; }
        /// <summary>
        /// 股票购买状态，true是买入状态
        /// </summary>
        public bool StockIsBuyState { get; set; }

        /// <summary>
        /// 交易日期
        /// </summary>
        public DateTime Date { get; set; }
    }
}