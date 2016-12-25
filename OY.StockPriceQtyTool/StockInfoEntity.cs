namespace OY.StockPriceQtyTool
{
    public class StockInfoEntity
    {
        /// <summary>
        /// 股票编码
        /// </summary>
        public string StockCode { get; set; }
        /// <summary>
        /// 股票前缀
        /// </summary>
        public string StockPrefix => StockCode.IsNotNullOrEmpty() ? StockCode.Substring(0, 2) : string.Empty;
    }
}