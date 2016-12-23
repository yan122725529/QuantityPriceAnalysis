using System;
using System.ComponentModel;

namespace OY.StockPriceQtyTool.Thread
{
    public sealed class WpfBackgroundWorker
    {
      

        public static BackgroundWorker RunWorkerAsync(Action<object,DoWorkEventArgs> doWork,Action<object,RunWorkerCompletedEventArgs> runWorkCompleted)
        {
            //支持取消
            var backgroundWorker = new BackgroundWorker(){WorkerSupportsCancellation = true};
            backgroundWorker.DoWork += (sender, args) =>
                                           {
                                               try
                                               {
                                                   doWork(sender, args);
                                               }
                                               catch (Exception ex)
                                               {
                                                   throw ex;
                                               }
                                           };
            backgroundWorker.RunWorkerCompleted += (s, e) => runWorkCompleted(s, e);
            backgroundWorker.RunWorkerAsync();
            return backgroundWorker;
        }
    }
}