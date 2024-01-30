using System.IO;
using System;

namespace Logic
{
    public static class Logger
    {
        public static StreamWriter swLog;

        public static void  Log(string strMessage){
            swLog = new StreamWriter(AppDomain.CurrentDomain.BaseDirectory+"log.txt", true);
            swLog.AutoFlush = true;
            swLog.WriteLine(strMessage);
            swLog.Close();
            Console.WriteLine(strMessage);
        }
    }
}