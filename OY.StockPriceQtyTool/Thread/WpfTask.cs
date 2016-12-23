using System;
using System.Threading.Tasks;

namespace OY.StockPriceQtyTool.Thread
{
    public static class WpfTask
    {
        public static Task FactoryStartNew(Action action)
        {
            return Task.Factory.StartNew(() =>
            {
                var errgiid = Guid.NewGuid().ToString(); //生成一个ERRID记录在日志,方便在日志中定义线程错误
                CatchForAction.ExceptionToUiThread("Factory Task Error" + Environment.NewLine + errgiid, action);
            });
        }
    }

    
 
}