using System;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Windows.Threading;

namespace OY.StockPriceQtyTool.Thread
{
    public static class CatchForAction
    {
        public static void ExceptionToUiThread(string errorTitle, Action action)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                 //todo 如果传递EX，错误堆栈可能有误
                CatchThreadException(errorTitle, ex);
            }
        }

        public static void ExceptionToDoWork(string errorTitle, Action<object, DoWorkEventArgs> action, object sender,
            DoWorkEventArgs args)
        {
            try
            {
                action(sender, args);
            }
            catch (Exception ex)
            {
                CatchThreadException(errorTitle, ex);
            }
        }

        private static void CatchThreadException(string errorTitle, Exception ex)
        {
            var t = System.Threading.Thread.CurrentThread;
            UiThread.Invoke(() =>
            {
                t.Abort();
                LogInnerEx(errorTitle, ex);
                throw ex; //抛出线程中的错误到全局未处理错误处理
            }, DispatcherPriority.Send);
        }

        /// <summary>
        ///     记录错误内的INNEREX
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="errguid"></param>
        private static void LogInnerEx(string errorTitle, Exception ex)
        {
            var aggEx = ex as AggregateException;
            var innerrrs = aggEx?.InnerExceptions;
            if (innerrrs == null || !innerrrs.Any()) return;
            foreach (var innerex in innerrrs)
            {
                //todo ZilLionLoger.WriteErrLog(ex);
            }
            return;
            //todo ZilLionLoger.WriteErrLog(ex);
        }

        public static string GetErrorString(Exception ex)
        {
            try
            {
                var logStringBuilder = new StringBuilder();

                logStringBuilder.Append(string.Format("System.Reflection.TargetInvocationException: {0} \r\n",
                    ex.Message));
                var outerStackTrace = ex.StackTrace;
                ex = ex.InnerException;
                while (ex != null)
                {
                    logStringBuilder.Append(string.Format("{0} \r\n", ex));
                    ex = ex.InnerException;
                }
                logStringBuilder.Append(string.Format(" --- End of inner exception stack trace --- \r\n {0}",
                    outerStackTrace));
                var CurrentLogMessage = logStringBuilder.ToString();


                return CurrentLogMessage;
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }
    }
}