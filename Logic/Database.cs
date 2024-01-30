using System.Xml;
using System.Linq.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
//using System.Data.conn;
using Npgsql;
using System.Data;
using System.Text.RegularExpressions;
using System.Globalization;

namespace Logic
{
    class ProcessSQL
    {
        
        private string strconnPath=AppDomain.CurrentDomain.BaseDirectory+"conn.db";
        private string connString;
        public ProcessSQL()
        {
            connString = "Host=127.0.0.1;Username=postgres;Password=postgres01;Database=monitor;";
        }
        public void CheckAndRecreateTables()
        {
            using (var vConn = new NpgsqlConnection(connString)){
                vConn.Open();
                                
                using (NpgsqlCommand cmdCreate = new NpgsqlCommand(
                        " CREATE TABLE if not exists apps (name text , username text,max_time int, day_of_the_week text, primary key (name,username,day_of_the_week)); " +
                        " CREATE TABLE if not exists daily_apps (pid int,app text,username text,start_time timestamp,end_time timestamp,primary key(pid, app));" +
                        " CREATE TABLE if not exists hist_apps (pid int,app text,username text,start_time timestamp,end_time timestamp);"+
                        " create table if not exists activetime (username text, max_time int, day_of_the_week text, last_time_connected timestamp, seconds_today int, primary key(username,day_of_the_week));" +
                        " create table if not exists logouts (username text, hour_min text, day_of_the_week text, primary key(username,day_of_the_week));" +
                        " create table if not exists logins (username text, hour_min text, day_of_the_week text, primary key(username,day_of_the_week));" +
                        " create table if not exists logoutsnow (username text primary key, day timestamp);"+
                        " comment on table apps is 'List of rules between apps & users'; "+
                        " comment on table daily_apps is 'Tracking of the start-end apps executed from the last start of Monitor';"+
                        " comment on table hist_apps is 'Historic of daily_apps table';"+
                        " comment on table activetime is 'Screen time granted to an user';"+
                        " comment on table logins is 'Time when user can start to spend his Screen time';"+
                        " comment on table logouts is 'Last time when user can enjoy his Screen Time';"+
                        " comment on table logoutsnow is 'To force a user to quit now';"+
                        " delete from logoutsnow where day<date_trunc('day',now());", vConn)){
                    cmdCreate.ExecuteNonQuery();
                }
                using (NpgsqlCommand cmdCreate = new NpgsqlCommand(
                       @"CREATE OR REPLACE FUNCTION minutes_for_username (username_ text) 
                        RETURNS INT AS $$
                    DECLARE
                        min_start_time timestamp;
                        cnt int;
                        i int;
                    begin
                        drop table if exists temp_minutes;
                        create temporary table temp_minutes (minute int);
                        --hora minima de la primera aplicacion abierta por el usuario
                        select min(start_time) into min_start_time from (
                            select min(start_time) as start_time from daily_apps da  where username=username_
                            union all
                            select min(start_time) as start_time from hist_apps ha where start_time >date_trunc('day',now()) and username=username_
                        )t;
                        -- iter by all the minutes starting from the min_start_time
                        for i in 1..(
                            SELECT abs((DATE_PART('day', min_start_time::timestamp - now()::timestamp) * 24 + 
                                DATE_PART('hour', min_start_time::timestamp - now()::timestamp)) * 60 +
                                DATE_PART('minute', min_start_time::timestamp - now()::timestamp)
                        ))::int loop
                            select count(*) into cnt from ( 
                                select * from daily_apps where username=username_ 
                                    and (min_start_time+ ( i||' minutes')::interval) >=start_time
                                    and (min_start_time+ ( i||' minutes')::interval) <=end_time
                                    and end_time is not null
                                union all 
                                select * from hist_apps where username=username_ 
                                    and (min_start_time+ ( i||' minutes')::interval) >=start_time
                                    and (min_start_time+ ( i||' minutes')::interval) <=end_time
                                    and end_time is not null 
                                    and start_time >date_trunc('day',now())
                                    )t;
                            if cnt>0 THEN
                                insert into temp_minutes values (i);
                                continue;
                            end if;
                            select count(*) into cnt from (
                                select * from daily_apps where username=username_ 
                                    and (min_start_time+ ( i||' minutes')::interval) >=start_time
                                    and (min_start_time+ ( i||' minutes')::interval) <=now()
                                    and end_time is null
                                union all
                                select * from hist_apps where username=username_ 
                                    and (min_start_time+ ( i||' minutes')::interval) >=start_time
                                    and (min_start_time+ ( i||' minutes')::interval) <=now()
                                    and end_time is null 
                                    and start_time >date_trunc('day',now())
                            )t;
                            if cnt>0 THEN
                                insert into temp_minutes values (i);
                                continue;
                            end if;
                        end loop;
                        select count(*) into cnt from temp_minutes;
                        return cnt;
                    exception
                    when others then
                        return 0;
                    END;
                    $$ LANGUAGE plpgsql;",vConn)){
                    cmdCreate.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// closes previous not closed (endtime null) apps and move rows to hist table
        /// </summary>
        /// <value></value>
        public void HistApps(){
            //NpgsqlConnection conn;
            //connDataAdapter ad;
            //NpgsqlCommand cmd;

            using (var conn = new NpgsqlConnection(connString)){
                conn.Open();

                using (NpgsqlCommand cmd = new NpgsqlCommand("update daily_apps set end_time=(select max(start_time) from daily_apps) where end_time is null",conn))
                {
                
                    cmd.ExecuteNonQuery();
                }
                using (NpgsqlCommand cmd = new NpgsqlCommand("insert into  hist_apps select * from daily_apps",conn))
                {
                    cmd.ExecuteNonQuery();
                }
                //delete until one month
                using (NpgsqlCommand cmd = new NpgsqlCommand("delete from hist_apps where start_time < now()- interval '1 month'",conn))
                {

                    cmd.ExecuteNonQuery();
                }
                ClearTable("daily_apps");
            }
        }

        
        /// <summary>
        /// Deletes the specified table
        /// </summary>
        /// <param name="strTable"></param> Table to delete
        public void ClearTable(string strTable){
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"truncate table {strTable}",conn))
                    cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Adds on a processpersist object all the records from daily_apps table
        /// </summary>
        /// <param name="p"></param>    
        public void GetDailyApps(ref List<ProcessesPersist> lp){
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand("select pid,app,coalesce(username,'no_username') from daily_apps where end_time is null",conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read()){
                        lp.Add(new ProcessesPersist(dr.GetInt32(0),dr.GetString(1),dr.GetString(2)));
                    }
                }
            }
        }

        /// <summary>
        /// Adds an app to the list of timed/disabled apps
        /// </summary>
        /// <param name="strAppName"></param>
        /// <param name="nMaxTime"></param>
        public bool AddApplication(string strAppName, string userName,int nMaxTime,string dayOfTheWeek)
        {
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"insert into apps (name,username,max_time,day_of_the_week) values ('{strAppName}','{userName}','{nMaxTime}','{dayOfTheWeek}')",conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        if (e.Message.Contains(Npgsql.PostgresErrorCodes.LockNotAvailable))
                        {
                            //Logger.Log($@"{DateTime.Now} [LOCK]: {cmd.CommandText}");
                            return false;
                        }
                        return false;
                    }
                }
            }
        }

        /// <summary>
        /// Remove app from the list of timed/disabled apps
        /// </summary>
        /// <param name="strAppName"></param>
        public bool RemoveApplicationFromUser(string appName,string userName, string dayOfTheWeek)
        {


            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"delete from apps where name ='{appName}' and username='{userName}' and lower(day_of_the_week)='{dayOfTheWeek.ToLower()}';",conn))
                {
                try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        if (e.Message.Contains(Npgsql.PostgresErrorCodes.LockNotAvailable))
                        {
                            //Logger.Log($@"{DateTime.Now} [LOCK]: {cmd.CommandText}");
                            return false;
                        }
                        return true;
                    }
                }
            }
            
        }

        /// <summary>
        ///  Get a list of the timed/disabled apps
        /// </summary>
        /// <returns></returns>
        public List<AppsPersist> GetApps(){

            List<AppsPersist> l=new List<AppsPersist>();

            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand("select name,username, max_time,day_of_the_week from apps",conn))
                {
                    NpgsqlDataReader dr;
                    try
                    {
                        dr = cmd.ExecuteReader();
                        while (dr.Read())
                        {
                            l.Add(new AppsPersist(dr.GetString(0), dr.GetString(1), dr.GetInt32(2), dr.GetString(3)));
                        }
                    }
                    catch (NpgsqlException e)
                    {
                        if (e.Message.Contains(Npgsql.PostgresErrorCodes.LockNotAvailable))
                        {
                            //Logger.Log($@"{DateTime.Now} [LOCK]: {cmd.CommandText}");
                        }
                        return l;

                    }
                }
            }
            return l;
        }

        /// <summary>
        /// Get some app minutes active during current day
        /// </summary>
        /// <param name="strAppName"></param>
        /// <returns></returns>
        public int GetActiveTimeByAppAndUser(string strAppName, string strUser){

            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select coalesce(sum(minutes)::integer,0) from(
                                        select
                                        extract (epoch from (coalesce(end_time, now()) - start_time))/60 as minutes
                                        from daily_apps da where app ='{strAppName}' and username='{strUser}'
                                        union all
                                        select 
                                        extract (epoch from (coalesce(end_time, now()) - start_time))/60 as minutes
                                        from hist_apps da where app ='{strAppName}' and username='{strUser}' and start_time >date_trunc('day',now())
                                    )t",conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        return dr.GetInt32(0);
                
                }
            }
            return 0;
        }

       
        public List<AppsPersist> GetCurrentDayAppUsage(){

            List<AppsPersist> lap=new List<AppsPersist>();

            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select app,username,sum(coalesce(minutes,0))::integer from (
	                                        select app,username,
	                                        extract(epoch from (coalesce(end_time, now()) - start_time))/60 as minutes 
                                            from daily_apps 
	                                        union all
	                                        select app,username,
	                                        extract(epoch from (coalesce(end_time, now()) - start_time))/ 60 as minutes 
                                            from hist_apps where start_time > now()
                                        )t
                                        group by app,username order by 3 desc limit 10",conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        lap.Add(new AppsPersist(dr.GetString(0),dr.GetString(1),dr.GetInt32(2), dr.GetString(3)));
                    return lap;
                }
            }
        }
        public List<AppsPersist> GetUsersWithActiveTimeExhausted(){

            List<AppsPersist> lap=new List<AppsPersist>();

            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username,day_of_the_week,max_time,minutes_for_username(username) from activetime
                                                                where lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')));",conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        if (dr.GetInt32(3)>=dr.GetInt32(2))
                            lap.Add(new AppsPersist(null, dr.GetString(0), 0, dr.GetString(1)));
                        Logger.Log($@"{DateTime.Now} [INFO]: Time consumed by {dr.GetString(0)}: {dr.GetInt32(3)}");

                    }

                }
            }
            return lap;
        }
        public List<AppsPersist> GetConfiguredLogouts()
        {
            
            List<AppsPersist> lap=new List<AppsPersist>();

            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username,hour_min, day_of_the_week from logouts;",conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        lap.Add(new AppsPersist(null,dr.GetString(0),System.Convert.ToInt32(dr.GetString(1)), dr.GetString(2)));
                    return lap;
                }
            }
        }
        public List<AppsPersist> GetConfiguredLogins()
        {
            
            List<AppsPersist> lap=new List<AppsPersist>();

            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username,hour_min, day_of_the_week from logins;",conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        lap.Add(new AppsPersist(null,dr.GetString(0),System.Convert.ToInt32(dr.GetString(1)), dr.GetString(2)));
                    return lap;
                }
            }
        }
        public bool AddLogout(string userName, int hour_min, string dayOfTheWeek)
        {
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"insert into logouts values ('{userName}',lpad({hour_min}::text,4,'0'),'{dayOfTheWeek}');",conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        //Logger.Log($@"{DateTime.Now} [ERROR]: inserting logout for {userName}");
                    }
                    return false;
                }
            }
        }
        public bool AddLogoutNow(string userName)
        {
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"insert into logoutsnow values ('{userName}',date_trunc('day',now()) )
                                                                    on conflict(username) do update set day=date_trunc('day',now());",conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        //Logger.Log($@"{DateTime.Now} [ERROR]: inserting logout for {userName}");
                    }
                    return false;
                }
            }
        }

        public bool RemoveLogoutNow(string userName)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"delete from logoutsnow where username='{userName}';", conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        //Logger.Log($@"{DateTime.Now} [ERROR]: inserting logout for {userName}");
                    }
                    return false;
                }
            }
        }
        public bool AddLogin(string userName, int hour_min, string dayOfTheWeek)
        {
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"insert into logins values ('{userName}',lpad({hour_min}::text,4,'0'),'{dayOfTheWeek}');",conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        //Logger.Log($@"{DateTime.Now} [ERROR]: inserting logout for {userName}");
                    }
                    return false;
                }
            }
        }
        public bool RemoveLogout(string userName, string dayOfTheWeek)
        {
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"delete from logouts where username ='{userName}' and lower(day_of_the_week)='{dayOfTheWeek.ToLower()}';",conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        //Logger.Log($@"{DateTime.Now} [ERROR]: Removing logout {userName}");
                    }
                    return false;
                }
            }
        }
        public bool RemoveLogin(string userName,string dayOfTheWeek)
        {
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"delete from logins where username ='{userName}' and lower(day_of_the_week)='{dayOfTheWeek.ToLower()}';",conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        //Logger.Log($@"{DateTime.Now} [ERROR]: Removing logout {userName}");
                    }
                    return false;
                }
            }
        }
        public bool RemoveActiveTime(string userName, string dayOfTheWeek)
        {
            Logger.Log($@"[RemoveActiveTime] username: {userName} dayoftheweek: {dayOfTheWeek}");
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"delete from activetime where username ='{userName}' and lower(day_of_the_week)='{dayOfTheWeek.ToLower()}';",conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        //Logger.Log($@"{DateTime.Now} [ERROR]: Removing logout {userName}");
                    }
                    return false;
                }
            }
        }
        public bool AddActiveTime(string userName, int maxTime, string dayOfTheWeek)
        {
            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"insert into activetime values ('{userName}',{maxTime},'{dayOfTheWeek}',null,0)",conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (NpgsqlException e)
                    {
                        //Logger.Log($@"{DateTime.Now} [ERROR]: Removing logout {userName}");
                    }
                    return false;
                }
            }
        }
        public List<AppsPersist> ListActiveTime()
        {
            
            List<AppsPersist> lap=new List<AppsPersist>();

            using (var conn = new NpgsqlConnection(connString))
            {                
                conn.Open();  
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username,max_time,day_of_the_week from activetime;",conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        lap.Add(new AppsPersist(null,dr.GetString(0),dr.GetInt32(1), dr.GetString(2)));
                    return lap;
                }
            }
        }
        /// <summary>
        /// This method updates or inserts the app on db.false Also updates the start and end Time
        /// </summary>
        /// <param name="strApp"></param>
        /// <param name="nPid"></param>
        /// <param name="dtEndTime"></param>
        public void UpdateApp(string strApp, string userName,int nPid)
        {
            //if (Monitor.IgnoredApps.Contains(strApp))
            //{
            //    Logger.Log($@"{DateTime.Now} [INFO] Ignored app {strApp}");
            //}
            //else //only insert the app (or update) in case the app is permitted
            //{


                using (var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand($@"INSERT INTO daily_apps(app,username,start_time,pid) VALUES('{strApp}','{userName}','{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}',{nPid})", conn))
                    {
                        try
                        {
                            cmd.ExecuteNonQuery();
                        }//si no es una nueva app saltará al catch

                        catch (NpgsqlException e)
                        {
                            if (e.Message.Contains(Npgsql.PostgresErrorCodes.LockNotAvailable))
                            {
                                //Logger.Log($@"{DateTime.Now} [LOCK]: {cmd.CommandText}");
                                //return false;
                            }
                            using (NpgsqlCommand cmdUpd = new NpgsqlCommand($@"update daily_apps set end_time='{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' where app='{strApp}' and pid={nPid}", conn))
                            {
                                cmdUpd.ExecuteNonQuery();
                            }
                        }
                    }
                }
            //}
        }

        public List<string> GetUsers()
        {
            List<string> users = new List<string>();
            //users on time > than logout
            using (var vConn = new NpgsqlConnection(connString))
            {
                vConn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username from activetime 
                            where lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')))", vConn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        users.Add(dr.GetString(0));
                }
            }
            return users;
        }
        public List<string> GetUsersToLogOut()
        {
            List<string> usersToLogOut=new List<string>();
            //users on time > than logout
            using (var vConn = new NpgsqlConnection(connString))
            {
                vConn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username from logouts 
                     where now()>=date_trunc('day',now() )+(substring(hour_min from 1 for 2)||' hour')::interval+(substring(hour_min from 3 for 2)||' minutes')::interval
                     and lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')));", vConn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        usersToLogOut.Add(dr.GetString(0));
                }
            }
            //users on time < than login time
            using (var vConn = new NpgsqlConnection(connString))
            {
            vConn.Open();
            using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username from logins 
                    where now()<date_trunc('day',now() )+(substring(hour_min from 1 for 2)||' hour')::interval+(substring(hour_min from 3 for 2)||' minutes')::interval
                    and lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')));", vConn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        usersToLogOut.Add(dr.GetString(0));
                }
            }
            //users to logoutnow
            using (var vConn = new NpgsqlConnection(connString))
            {
            vConn.Open();
            using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username from logoutsnow;", vConn))
                    //where now()>date_trunc('day',now() )+(substring(hour_min from 1 for 2)||' hour')::interval+(substring(hour_min from 3 for 2)||' minutes')::interval;",vConn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        usersToLogOut.Add(dr.GetString(0));
                }
            }
            //users that consumed all their time
            //foreach (AppsPersist appPersist in GetUsersWithActiveTimeExhausted().Where(x=>x._dayOfTheWeek.ToLower()== DateTime.Today.DayOfWeek.ToString().ToLower()))
            //    usersToLogOut.Add(appPersist._userName);
            using (var vConn = new NpgsqlConnection(connString))
            {
                vConn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username from activetime
                     where seconds_today>max_time*60
                     and lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')));", vConn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                        usersToLogOut.Add(dr.GetString(0));
                }
            }

            return usersToLogOut;
        }
        public void UpdateSessionTime(string userName)
        {
            Logger.Log($@"[UpdateSessionTime] username: {userName}");
            using (var vConn = new NpgsqlConnection(connString))
            {
                DateTime lastTimeConnected;
                vConn.Open();

                //using (NpgsqlCommand cmd = new NpgsqlCommand($@"select seconds_today from activetime where username='{userName}'
                //                                                and lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')))", vConn))
                //{
                //    Logger.Log($@"{DateTime.Now} [INFO]: Seconds consumed by {userName}: {((int)cmd.ExecuteScalar())}");
                //}

                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select coalesce(last_time_connected,now()) from activetime where username='{userName}'
                                                                and lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')))", vConn))
                {
                    lastTimeConnected = ((DateTime)cmd.ExecuteScalar()).ToLocalTime();
                }
                int secondsConsumedFromLastIteration=(int)(DateTime.Now - lastTimeConnected).TotalSeconds;
                if (secondsConsumedFromLastIteration < 60) //probably continues session
                {
                    using (NpgsqlCommand cmd = new NpgsqlCommand($@"update activetime set last_time_connected='{DateTime.Now}', 
                                                                    seconds_today=seconds_today+'{secondsConsumedFromLastIteration}' 
                                                                    where username='{userName}'
                                                                    and lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')))", vConn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                else //only update last_time, next iteration will increase the time consumed
                {
                    using (NpgsqlCommand cmd = new NpgsqlCommand($@"update activetime set last_time_connected='{DateTime.Now}' 
                        where username='{userName}'
                        and lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')))", vConn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }
        /// <summary>
        /// Reset ammount of active time consumed by a session (only if we are on other day than previous active time, this is to contimue time 
        /// if the machine reboots). Also if the last_time_connected is null.
        /// </summary>
        public void ReStartConsumedTimeFromUsers()
        {
            Logger.Log($@"[ReStartConsumedTimeFromUsers]");
            using (var vConn = new NpgsqlConnection(connString))
            {
                vConn.Open();

                using (NpgsqlCommand cmd = new NpgsqlCommand($@"update activetime set last_time_connected=null,seconds_today=0 
                                                                    where (last_time_connected<date_trunc('day',now())
                                                                    or last_time_connected is null)", vConn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public List<AppsPersist> GetUsersRemainingTime()
        {
            List<AppsPersist> lap = new List<AppsPersist>();

            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username,max_time-seconds_today/60 from activetime
                                                                where lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')));", conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        lap.Add(new AppsPersist(null, dr.GetString(0), dr.GetInt32(1),null));
                    }

                }
            }
            return lap;
        }
        public AppsPersist GetUserRemainingTime(string userName)
        {
            AppsPersist ap = new AppsPersist();

            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username,max_time-seconds_today/60 from activetime
                                                                where lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')))
                                                                and username='{userName}';", conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    try
                    {
                        while (dr.Read())
                        {
                            ap = new AppsPersist(null, dr.GetString(0), dr.GetInt32(1), null);
                        }
                    }
                    catch (Exception e)
                    {
                        ap = new AppsPersist(null, userName, 0, null);
                    }
                }
            }
            return ap;
        }
        public int GetUserConsumedSeconds(string userName)
        {
            AppsPersist ap = new AppsPersist();

            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select username,seconds_today from activetime
                                                                where lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')))
                                                                and username='{userName}';", conn))
                {
                    NpgsqlDataReader dr;
                    dr = cmd.ExecuteReader();
                    try
                    {
                        while (dr.Read())
                        {
                            ap = new AppsPersist(null, dr.GetString(0), dr.GetInt32(1), null);
                        }
                    }
                    catch (Exception e)
                    {
                        ap = new AppsPersist(null, userName, 0, null);
                    }
                }
            }
            return ap._time;
        }
        public bool SetUserConsumedSeconds(string userName, int seconds)
        {
            Logger.Log($@"[SetUserConsumedSeconds] username: {userName} seconds: {seconds}");
            AppsPersist ap = new AppsPersist();

            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"update activetime set seconds_today={seconds}
                                                                where lower(day_of_the_week)=rtrim(lower(to_char(now(),'day')))
                                                                and username='{userName}';", conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                        return true;
                    }
                    catch (Exception e)
                    {
                        return false;
                    }
                }
            }
        }
        public bool GetLogougNowStatusForUser(string userName)
        {
            Logger.Log($@"[GetLogougNowStatusForUser] username: {userName}");
            AppsPersist ap = new AppsPersist();

            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand($@"select count(*) from logoutsnow where username like '{userName}'", conn))
                {
                    return (System.Int64) cmd.ExecuteScalar() == 1;
                }
            }
        }
    }
}