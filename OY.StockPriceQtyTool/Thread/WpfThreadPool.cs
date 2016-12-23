using System.Threading;

namespace OY.StockPriceQtyTool.Thread
{
    public static class WpfThreadPool
    {
        public static void QueueUserWorkItem(WaitCallback callback)
        {
            ThreadPool.QueueUserWorkItem(x => CatchForAction.ExceptionToUiThread("WpfThreadPool Error", () => callback(x)));
        }

        public static void QueueUserWorkItem(WaitCallback callback, object state)
        {
            ThreadPool.QueueUserWorkItem(x => CatchForAction.ExceptionToUiThread("WpfThreadPool Error", () => callback(state)));
        }

        public static void UnsafeQueueUserWorkItem(WaitCallback callback, object state)
        {
            ThreadPool.UnsafeQueueUserWorkItem(x => CatchForAction.ExceptionToUiThread("WpfThreadPool Error", () => callback(state)), null);
        }
    }
}