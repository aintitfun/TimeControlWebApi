namespace Logic
{
    public class AppsPersist 
    {        
        public int _command;
        public string _app;
        public string _userName;
        public int _time;
        public string _dayOfTheWeek;
        

        public AppsPersist( )
        {
            
        }
        public AppsPersist(string app,string userName,int time, string dayOfTheWeek)
        {
            this._app= app;
            this._userName=userName;
            this._time= time;
            this._dayOfTheWeek = dayOfTheWeek;
        }
    }

    public class ProcessesPersist 
    {
        public string ProcessName;
        public int Id;
        public string User;
        public ProcessesPersist(int nPid,string name, string username )

        {
            Id=nPid;
            ProcessName=name;
            User=username;
            
        }
    }
}