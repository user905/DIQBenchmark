using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using DotNetEnv;

namespace DBFunctionPerfTest
{
    class Program
    {
        static void Main()
        {
            Env.Load();
            string connectionString = Environment.GetEnvironmentVariable("DATABASE_URL_TEST");
            int uploadId = 1981;
            var functionList = new[] 
            {
                "fnDIQ_DS06_Res_IsEOCComboMisalignedWithDS03",
                "efnDIQ_DS06_Res_IsEOCComboMisalignedWithDS03",
                "fnDIQ_DS06_Res_DoNonLaborSHoursExistWithDS03LaborSHours",
                "efnDIQ_DS06_Res_DoNonLaborSHoursExistWithDS03LaborSHours",
                "fnDIQ_DS06_Res_AreSDollarsMisalignedWithDS03SDollars",
                "efnDIQ_DS06_Res_AreSDollarsMisalignedWithDS03SDollars",
                "fnDIQ_DS06_Res_AreRemUnitsMisalignedWithDS03BCWRHours",
                "efnDIQ_DS06_Res_AreRemUnitsMisalignedWithDS03BCWRHours",
                "fnDIQ_DS06_Res_AreRemDollarsMisalignedWithDS03BCWR",
                "efnDIQ_DS06_Res_AreRemDollarsMisalignedWithDS03BCWR",
                "fnDIQ_DS06_Res_ArePDollarsMisalignedWithDS03",
                "efnDIQ_DS06_Res_ArePDollarsMisalignedWithDS03",
                "fnDIQ_DS06_Res_AreLaborSUnitsMisalignedWithDS03LaborSHours",
                "efnDIQ_DS06_Res_AreLaborSUnitsMisalignedWithDS03LaborSHours",
                "fnDIQ_DS06_Res_AreLaborPUnitsMisalignedWithDS03LaborPHours",
                "efnDIQ_DS06_Res_AreLaborPUnitsMisalignedWithDS03LaborPHours",
                "fnDIQ_DS03_Cost_IsPeriodAfterPMBEnd",
                "efnDIQ_DS03_Cost_IsPeriodAfterPMBEnd",
                "fnDIQ_DS06_Res_ArePDollarsMissingDS03ADollarsWP",
                "efnDIQ_DS06_Res_ArePDollarsMissingDS03ADollarsWP",
                "fnDIQ_DS06_Res_AreLaborPUnitsMissingDS03LaborAHoursWP",
                "efnDIQ_DS06_Res_AreLaborPUnitsMissingDS03LaborAHoursWP",
                "fnDIQ_DS06_Res_AreDS03ADollarsMissingResourcePDollarsWP",
                "efnDIQ_DS06_Res_AreDS03ADollarsMissingResourcePDollarsWP" 
            };

            // Set up log file
            string logFilePath = $"./{uploadId}_executionTimes.log";
            StreamWriter logFile = File.AppendText(logFilePath);

            using (SqlConnection connection = new(connectionString))
            {
                connection.Open();
            
                RunAndLogProcedure("[dbo].[LoadWBSRollup_WP]", connection, uploadId, logFile);
                
                foreach(var functionName in functionList)
                {
                    RunAndLogFunction(functionName, connection, uploadId, logFile);
                }

                connection.Close();
            }

            logFile.Close();
        }

        static void RunAndLogProcedure(string procedureName, SqlConnection connection, int uploadId, StreamWriter logFile)
        {
            SqlCommand cmd = new(procedureName, connection)
            {
                CommandType = CommandType.StoredProcedure
            };
        
            cmd.Parameters.Add(new SqlParameter("@upload_ID", uploadId));
    
            Stopwatch stopwatch = Stopwatch.StartNew();
            cmd.ExecuteNonQuery();
            stopwatch.Stop();

            logFile.WriteLine($"{procedureName} executed in {stopwatch.ElapsedMilliseconds} ms");
        }

        static void RunAndLogFunction(string functionName, SqlConnection connection, int uploadId, StreamWriter logFile)
        {
            string query = $"select * from {functionName}(@upload_id)";

            SqlCommand cmd = new(query, connection);
            cmd.CommandTimeout = 60 * 30;
            cmd.Parameters.Add(new SqlParameter("@upload_id", uploadId));

            Stopwatch stopwatch = Stopwatch.StartNew();
            SqlDataReader reader = cmd.ExecuteReader();
            stopwatch.Stop();

            logFile.WriteLine($"{functionName} executed in {stopwatch.ElapsedMilliseconds} ms");

            reader.Close();
        }
    }
}
