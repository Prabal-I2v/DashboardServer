using Dashboard.Models;

namespace BusinessLayer.QueryUtils
{
    public static class WidgetsOutputDataModel
    {
        public static ChartsOutputModel CreateOutPutModel(WidgetRequestModel widgetInputModel, List<dynamic> queryResult)
        {
            return null;
        }
    }
}

public class ChartsOutputModel
{
    public String[] Labels { get; set; }
    public ChartsDataModel[] Data { get; set; }
}

public class ChartsDataModel
{
    public string[] Data { get; set; }
}