namespace Dashboard.Models;

public class WidgetRequestModel
{
    public WidgetRequestModel()
    {
        FieldName = new Dictionary<string, PropertyType>();
    }
    public int Id { get; set; }
    public long StartTime { get; set; }
    public long EndTime { get; set; }
    public Enum_WidgetType WidgetType { get; set; }
    public Enum_Entity Entity { get; set; }
    public Enum_Method Method { get; set; }
    //base filter is the event of which we want event like FRS, ANPR, ATCC
    public RuleSet BaseFilter { get; set; }
    //Column to selected for base filter
    public Dictionary<string, PropertyType> FieldName { get; set; }
    public string ClubbingFieldName { get; set; }
    public Enum_Schema SchemaName { get; set; }
    public string GroupBy1 { get; set; }
    public bool GroupByOneIsTime { get; set; }
    public bool GroupByTwoIsTime { get; set; }
    public string GroupBy2 { get; set; }
    public bool IsDistinct { get; set; }
    public bool ClubbingTime { get; set; }
    public bool Pagination { get; set; }
    public int PageLimit { get; set; }
    public int PageNumber { get; set; }
    public string IdentifierFieldName { get; set; }
    public int MultiplicationFactor { get; set; }
    public RuleSet? PropertyFilters { get; set; }
    public int RefreshInterval { get; set; }
    public bool? IsPreview { get; set; }
}

public enum Enum_Entity
{
    Events,
    VideoSources,
    Person,
    EnrolledPersonsEvent,
    Facepoint,
    Highway_ATCC,
    VIDS,
    Vehicle_Stopped,
    ANPR,
    Wrong_Way_Detected
}

public enum Enum_Method
{
    Count,
    Sum,
    Average,
    LiveCount
}

public enum Enum_WidgetType
{
    AreaChart,
    BarChart,
    ColumnChart,
    HeatMapChart,
    LineChart,
    PieChart,
    StackedBarChart,
    StackedColumnChart,
    KPI,
    Table

}

public class RuleSet
{
    public string Condition { get; set; }
    public ICollection<Rule> Rules { get; set; }
    public ICollection<RuleSet> Ruleset { get; set; }
}

public class Rule
{
    public string Field { get; set; }
    public string Operator { get; set; }
    public string Value { get; set; }
    public PropertyType Type { get; set; }
}

public enum PropertyType
{
    String,
    Number,
    SingleSelect,
    MultipleSelect,
    ImagePath,
    Boolean,
    NumberArray,
    StringArray,
    Base64Image,
    Integer,
    Float,
    IpAddress,
    DateTime,
    Image,
    Date,
    TimeOfDay,
    DayOfWeek,
    UNIXDateTime,
    FilePath,
    Array,
    Custom,
}

public enum Enum_Schema
{
    Public,
    Events
}
