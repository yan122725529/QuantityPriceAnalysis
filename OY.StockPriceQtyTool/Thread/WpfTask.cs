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
                var errgiid = Guid.NewGuid().ToString(); //����һ��ERRID��¼����־,��������־�ж����̴߳���
                CatchForAction.ExceptionToUiThread("Factory Task Error" + Environment.NewLine + errgiid, action);
            });
        }
    }

    
 
}