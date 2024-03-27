using BusinessLayer.QueryUtils;
using Dashboard.Models;
using Serilog;

namespace Dashboard.QueryUtils
{
    public static class QueryManager
    {
        private static int DataLimit = 200;
        static QueryManager()
        {
            
        }
        public static ChartsOutputModel GetQueryData(WidgetRequestModel WidgetRequestModel)
        {
            ChartsOutputModel queryResult = null;
            try
            {
                var query = QueryCreator.GetQuery(WidgetRequestModel);
                //Get Data from Query and output as ChartsOutPutModel
                Log.Debug($"final query  {query}");
            }
            catch (Exception ex)
            {
                // ExceptionHandler.HandleException(ex);
            }

            return queryResult;
        }
    }
}






