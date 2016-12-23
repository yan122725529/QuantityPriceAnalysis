using System;
using System.Windows;
using System.Windows.Threading;

namespace OY.StockPriceQtyTool.Thread
{
    public class UiThread
    {
        public static bool IsUiThread()
        {
            return
                Application.Current.Dispatcher.Thread.ManagedThreadId ==
                System.Threading.Thread.CurrentThread.ManagedThreadId;
        }

        public static void Invoke(Action action)
        {
            Application.Current.Dispatcher.Invoke(DispatcherPriority.Send, action);
        }

        public static void Invoke(Action action, DispatcherPriority priority)
        {
            Application.Current.Dispatcher.Invoke(priority, action);
        }

        public static void BeginInvoke(Action action)
        {
            Application.Current.Dispatcher.BeginInvoke(DispatcherPriority.Render, action);
        }

        public static void BeginInvoke(Action action, DispatcherPriority priority)
        {
            Application.Current.Dispatcher.BeginInvoke(priority, action);
        }
    }
}