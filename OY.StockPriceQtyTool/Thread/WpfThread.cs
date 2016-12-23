using System.Threading;

namespace OY.StockPriceQtyTool.Thread
{
    /// <summary>
    /// Default Thread is background
    /// </summary>
    public class WpfThread
    {
        public System.Threading.Thread BackgroundThread { get; set; }

        public WpfThread(ThreadStart threadStart)
        {
            BackgroundThread = new System.Threading.Thread(() => CatchForAction.ExceptionToUiThread("WpfThread Error", threadStart.Invoke)) { IsBackground = true };
        }

        public WpfThread(ParameterizedThreadStart parameterizedThreadStart)
        {
            BackgroundThread = new System.Threading.Thread(x => CatchForAction.ExceptionToUiThread("WpfThreadPool Error", () => parameterizedThreadStart.Invoke(x))) { IsBackground = true };
        }
    }
}