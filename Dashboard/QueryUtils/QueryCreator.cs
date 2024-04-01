using System.Text.RegularExpressions;
using Dashboard.Models;
using Serilog;

namespace BusinessLayer.QueryUtils
{
    public static class QueryCreator
    {
        private static readonly Dictionary<string, string> TimePeriodToItsClubMapping = new Dictionary<string, string>
        {
            { "hour", "hour" }, { "date", "date" }, { "month", "month" }, { "year", "year" },
            { "week", "week" } // this is for week of the year - if we need week of month we can use W
        };

        public static string GetQuery(WidgetRequestModel req)
        {
            string finalQuery = string.Empty;
            Dictionary<string, string> QueryPartNameToQueryMap = new Dictionary<string, string>();
            string Basequery = string.Empty;
            string aggregateMethodStartingPartQuery = String.Empty;
            string aggregateMethodEndPartQuery = string.Empty;
            string aggregateMethodGroupByOptionalPart = string.Empty;
            string whereClausePart = string.Empty;
            CreateBaseQueryWithRules(req, out Basequery, out whereClausePart);
            QueryPartNameToQueryMap.Add("baseQuery", Basequery);


            if (req.FieldName != null && req.FieldName.Count > 0)
            {
                switch (req.Method)
                {
                    case Enum_Method.Count:
                        QueryPartNameToQueryMap.Add("whereClausePart", whereClausePart);
                        QueryForCount(req, out aggregateMethodStartingPartQuery, out aggregateMethodEndPartQuery);
                        break;
                    case Enum_Method.Sum:
                        QueryForSum(req, out aggregateMethodStartingPartQuery, out aggregateMethodEndPartQuery, out aggregateMethodGroupByOptionalPart);
                        QueryPartNameToQueryMap.Add("whereClausePart", whereClausePart);
                        break;
                    // case Enum_Method.LiveCount:
                    //     QueryForLiveCount(req, out aggregateMethodStartingPartQuery, out aggregateMethodEndPartQuery,
                    //         out whereClausePart);
                    //     QueryPartNameToQueryMap.Add("whereClausePart", whereClausePart);
                    //     break;
                    // case Enum_Method.Average:
                    //     QueryForAverage(req, whereClausePart, out aggregateMethodStartingPartQuery,
                    //         out aggregateMethodEndPartQuery, out aggregateMethodGroupByOptionalPart,
                    //         out whereClausePart);
                    //     QueryPartNameToQueryMap.Add("whereClausePart", whereClausePart);
                    //     break;
                }

                QueryPartNameToQueryMap.Add("aggregateMethodStartingPartQuery", aggregateMethodStartingPartQuery);
                QueryPartNameToQueryMap.Add("aggregateMethodEndPartQuery", aggregateMethodEndPartQuery);
                QueryPartNameToQueryMap.Add("aggregateMethodGroupByOptionalPart", aggregateMethodGroupByOptionalPart);

                if (!string.IsNullOrEmpty(req.GroupBy1) && req.Entity != Enum_Entity.VideoSources)
                {
                    if (req.GroupByOneIsTime)
                    {
                        string groupBy1TimeStartPart = string.Empty;
                        string groupBy1TimeMidPart = string.Empty;
                        string groupBy1TimeEndPart = string.Empty;
                        QueryForGroupByTime(req.GroupBy1, out groupBy1TimeStartPart, out groupBy1TimeMidPart, out groupBy1TimeEndPart);
                        QueryPartNameToQueryMap.Add("groupBy1TimeStartPart", groupBy1TimeStartPart);
                        QueryPartNameToQueryMap.Add("groupBy1TimeMidPart", groupBy1TimeMidPart);
                        QueryPartNameToQueryMap.Add("groupBy1TimeEndPart", groupBy1TimeEndPart);
                    }
                    else
                    {
                        string groupBy1PropertyPart1 = string.Empty, groupBy1PropertyPart2 = string.Empty;
                        QueryForGroupByProperty(req.GroupBy1, out groupBy1PropertyPart1, out groupBy1PropertyPart2);
                        QueryPartNameToQueryMap.Add("groupBy1PropertyPart1", groupBy1PropertyPart1);
                        QueryPartNameToQueryMap.Add("groupBy1PropertyPart2", groupBy1PropertyPart2);
                    }
                }

                if (!string.IsNullOrEmpty(req.GroupBy2) && req.Entity != Enum_Entity.VideoSources)
                {
                    if (req.GroupByTwoIsTime)
                    {
                        string groupBy2TimeStartPart = string.Empty;
                        string groupBy2TimeMidPart = string.Empty;
                        string groupBy2TimeEndPart = string.Empty;
                        QueryForGroupByTime(req.GroupBy2, out groupBy2TimeStartPart, out groupBy2TimeMidPart, out groupBy2TimeEndPart);
                        QueryPartNameToQueryMap.Add("groupBy2TimeStartPart", groupBy2TimeStartPart);
                        QueryPartNameToQueryMap.Add("groupBy2TimeMidPart", groupBy2TimeMidPart);
                        QueryPartNameToQueryMap.Add("groupBy2TimeEndPart", groupBy2TimeEndPart);
                    }
                    else
                    {
                        string groupBy2PropertyPart1 = string.Empty, groupBy2PropertyPart2 = string.Empty;
                        QueryForGroupByProperty(req.GroupBy2, out groupBy2PropertyPart1, out groupBy2PropertyPart2);
                        QueryPartNameToQueryMap.Add("groupBy2PropertyPart1", groupBy2PropertyPart1);
                        QueryPartNameToQueryMap.Add("groupBy2PropertyPart2", groupBy2PropertyPart2);
                    }
                }
            }

            if (!QueryPartNameToQueryMap.ContainsKey("whereClausePart"))
            {
                QueryPartNameToQueryMap.Add("whereClausePart", whereClausePart);
            }

            finalQuery = JoinQuery(req, QueryPartNameToQueryMap);
            Log.Debug("final query" + " " + finalQuery);
            return finalQuery;
        }

        private static void CreateBaseQueryWithRules(WidgetRequestModel req, out string Basequery,
            out string whereClausePart)
        {
            string entityName = string.Empty;
            Basequery = string.Empty;
            whereClausePart = string.Empty;

            var startTime = req.StartTime;
            var endTime = req.EndTime;

            entityName = req.Entity.ToString();
            Basequery = $"SELECT * FROM {req.SchemaName.ToString().ToLower()}.\"{entityName}\" ";
            whereClausePart = $"\"Time\" >= '{startTime}' AND \"Time\" <= '{endTime}' ";


            if (req.BaseFilter != null)
            {
                string condition = "";
                if (!string.IsNullOrEmpty(whereClausePart))
                {
                    condition = "AND";
                }

                var aquery = ProcessRuleSet(req.BaseFilter, condition);
                if (aquery != "")
                {
                    if (aquery != "()")
                    {
                        whereClausePart += aquery;
                    }
                }
            }

            if (req.PropertyFilters != null)
            {
                string condition = "";
                if (!string.IsNullOrEmpty(whereClausePart))
                {
                    condition = "AND";
                }

                var aquery = ProcessRuleSet(req.PropertyFilters, condition);
                if (aquery != "")
                {
                    if (aquery != "()")
                    {
                        whereClausePart += aquery;
                    }
                }
            }
        }

        private static string ProcessRuleSet(RuleSet ruleSet, string condition, Boolean flagBracket = true,
            Boolean addConditionOrNot = true)
        {
            string query = "";
            if (addConditionOrNot)
            {
                query = $"{condition}";
            }

            if (flagBracket)
            {
                query += "(";
            }

            if (ruleSet.Rules?.Count > 0)
            {
                foreach (var it in ruleSet.Rules.Select((rule, Index) => new { rule, Index }))
                {
                    if (it.Index == 0 || it.Index == ruleSet.Rules.Count)
                    {
                        query += ProcessRule(it.rule);
                    }
                    else
                    {
                        query += $" {ruleSet.Condition} ";
                        query += ProcessRule(it.rule);
                    }
                }
            }

            if (ruleSet.Ruleset != null && ruleSet.Ruleset.Count > 0)
            {
                int index = 0;
                foreach (var tempRuleSet in ruleSet.Ruleset)
                {
                    if (index == 0)
                    {
                        query += ProcessRuleSet(tempRuleSet, ruleSet.Condition, true);
                        index++;
                    }
                    else
                    {
                        query += ProcessRuleSet(tempRuleSet, ruleSet.Condition, false);
                    }
                }
            }

            if (flagBracket)
            {
                query += ")";

                query = query.Replace("(or", "(");

                query = query.Replace("(and", "(");
            }

            return query;
        }

        private static string ProcessRule(Rule rule)
        {
            string query = "";
            if (!string.IsNullOrEmpty(rule.Value))
            {
                dynamic value = "";
                dynamic type = "";
                switch (rule.Type)
                {
                    case PropertyType.Number:
                        value = Convert.ToInt32(rule.Value);
                        type = "integer";
                        break;
                    case PropertyType.String:
                        value = rule.Value;
                        type = "text";
                        break;
                    case PropertyType.Array:
                        var list = rule.Value.Split(',');
                        foreach (var item in list)
                        {
                            if (value == "")
                            {
                                value = "'" + item + "'";
                            }
                            else
                            {
                                value += ",'" + item.ToString() + "'";
                            }
                        }

                        type = "text";
                        break;
                    case PropertyType.Float:
                        value = Convert.ToDouble(rule.Value);
                        type = "real";
                        break;
                    case PropertyType.Boolean:
                        value = Convert.ToBoolean(rule.Value);
                        type = "boolean";
                        break;
                }

                string FieldName = "";
                if (rule.Field.Contains("EventsProperties") || rule.Field.Contains("EventsProperties"))
                {
                    FieldName = $" \"_EventsProperties\" ->> '{rule.Field.Split('.')[1]}'";
                }
                else if (rule.Field.Contains("Properties") || rule.Field.Contains("properties"))
                {
                    FieldName = $" \"_Properties\" ->> '{rule.Field.Split('.')[1]}'";
                }
                else
                {
                    FieldName = $"\"{rule.Field}\"";
                }

                switch (rule.Operator)
                {
                    case "Equal":
                        query = $" {FieldName} = '{value}' ";
                        break;
                    case "NotEqual":
                        query = $" {FieldName} != '{value}' ";
                        break;
                    case "Contains":
                        query = $" {FieldName} IN ({value}) ";
                        break;
                    case "Not Contains":
                        query = $" {FieldName} NOT IN ({value}) ";
                        break;
                    case "LessThan":
                        query = $" ({FieldName})::{type} < {value}";
                        break;
                    case "GreaterThan":
                        query = $" ({FieldName})::{type} > {value}";
                        break;
                }
            }

            return query;
        }

        private static void QueryForCount(WidgetRequestModel req, out string countStartPartQuery,
            out string countEndPartQuery)
        {
            string columnName = req.FieldName.First().Key;
            countStartPartQuery = $"\"{columnName}\" as {columnName}, COUNT(\"{columnName}\") as count ";
            countEndPartQuery = $" Group By {columnName} ";
        }

        private static void QueryForLiveCount(WidgetRequestModel req, out string liveCountStartPartQuery,
            out string liveCountEndPartQuery, out string whereClausePart)
        {
            whereClausePart = "";
            liveCountEndPartQuery = "";
            var logDate = "Log_Date";
            var propertyNameColumn = "PropertyName";
            var startTime = req.StartTime;
            var endTime = req.EndTime;
            string logDateValueType = "Day Month dd YYYY HH24:MI:SS:MS";
            //if (req.GroupByOneIsTime == true) {
            //    switch (req.GroupBy1.ToLower())
            //    {
            //        case "month":
            //            logDateValueType = "Month";
            //            break;
            //        case "hour":
            //            logDateValueType = "hh am";
            //            break;
            //        case "year":
            //            logDateValueType = "YYYY";
            //            break;
            //        case "day":
            //            logDateValueType = "day";
            //            break;
            //        case "week":
            //            logDateValueType = "WW \"week\"";
            //            break;
            //    }
            //}
            liveCountStartPartQuery =
                $"\"{propertyNameColumn}\",\"NewValue\", TO_CHAR(TO_TIMESTAMP(\"{logDate}\" /1000) AT TIME ZONE 'IST', \'{logDateValueType}\') as Log_Date";
            //liveCountEndPartQuery = $"  inner join Group By a on t.\"{logDate}\" = a.\"max_date\"";
            //if (string.IsNullOrEmpty(req.GroupBy1))
            //{
            //    liveCountEndPartQuery += $"AND t.\"{propertyNameColumn}\"=a.\"p\" ";
            //}
            //if (string.IsNullOrEmpty(req.GroupBy1))
            //{
            //    whereClausePart = $"(SELECT MAX(\"{logDate}\") as max_date, \"{propertyNameColumn}\" as p FROM public.\"{tableName}\" WHERE";
            //}
            //else
            //{
            //    whereClausePart = $"(SELECT MAX(\"{logDate}\") as max_date, date_trunc('month', to_timestamp(\"{logDate}\"/1000)) FROM public.\"{tableName}\" WHERE";
            //}
            whereClausePart += $" Where (";
            foreach (KeyValuePair<string, PropertyType> entry in req.FieldName)
            {
                whereClausePart += $"\"{propertyNameColumn}\"=\'{entry.Key}\'";
                if (!req.FieldName[entry.Key].Equals(req.FieldName.Last().Key))
                {
                    whereClausePart += "or ";
                }
            }

            whereClausePart = whereClausePart.Substring(0, whereClausePart.Length - 3);
            whereClausePart += ") and \"NewValue\" != '' ";
            whereClausePart +=
                $"and TO_TIMESTAMP(\'{startTime}\', 'YYYY-MM-DD HH24:MI:SS') <= TO_TIMESTAMP(\"{logDate}\" / 1000)";
            whereClausePart +=
                $"and TO_TIMESTAMP(\'{endTime}\', 'YYYY-MM-DD HH24:MI:SS') >= TO_TIMESTAMP(\"{logDate}\" / 1000)";

            whereClausePart = addBaseorPropertyFilterQuery(req, whereClausePart);

            //    whereClausePart = whereClausePart.Substring(0, whereClausePart.Length - 3);
            //    if (string.IsNullOrEmpty(req.GroupBy1))
            //    {
            //        whereClausePart += $"Group BY \"{propertyNameColumn}\")";
            //    }
            //    else {
            //        whereClausePart += $"Group BY date_trunc('month', to_timestamp(\"{logDate}\"/1000)))";
            //    }
            //for (int totalProperties = 0; totalProperties < req.FieldName.Count; totalProperties++) {
            //    innerJoinQuery += $"\"{propertyNameColumn}\"=\'{ req.FieldName[0].Key.Split('.')[1]}\'";
            //}
            whereClausePart += $"order by \"{logDate}\"";
        }

        private static string addBaseorPropertyFilterQuery(WidgetRequestModel req, string whereClausePart)
        {
            if (req.BaseFilter != null)
            {
                string condition = "";
                if (!string.IsNullOrEmpty(whereClausePart))
                {
                    condition = "AND";
                }

                var aquery = ProcessRuleSet(req.BaseFilter, condition);
                if (aquery != "")
                {
                    if (aquery != "()")
                    {
                        whereClausePart += aquery;
                    }
                }
            }

            if (req.PropertyFilters != null)
            {
                var aquery = ProcessRuleSet(req.PropertyFilters, "AND");
                if (aquery != "")
                {
                    if (aquery != "()")
                    {
                        whereClausePart += aquery;
                    }
                }
            }

            return whereClausePart;
        }

        private static void SumOrAverageQueryForResourceDataLog(WidgetRequestModel req, string whereClause,
            bool forAverage, out string sumStartPartQuery, out string sumEndPartQuery,
            out string sumGroupByOptionalPart, out string whereClausePart)
        {
            sumGroupByOptionalPart = string.Empty;
            whereClausePart = "";
            sumEndPartQuery = "";
            var logDate = "Log_Date";
            var propertyNameColumn = "PropertyName";
            var startTime = req.StartTime;
            var endTime = req.EndTime;
            string logDateValueType = "Day Month dd YYYY HH24:MI:SS:MS";
            if (req.GroupByOneIsTime || req.GroupByTwoIsTime)
            {
                string switchCase = "";
                if (req.GroupByOneIsTime)
                {
                    switchCase = req.GroupBy1.ToLower();
                }
                else if (req.GroupByTwoIsTime)
                {
                    switchCase = req.GroupBy2.ToLower();
                }

                switch (switchCase)
                {
                    case "month":
                        logDateValueType = "Month";
                        break;
                    case "hour":
                        logDateValueType = "hh am";
                        break;
                    case "year":
                        logDateValueType = "YYYY";
                        break;
                    case "day":
                        logDateValueType = "dd day";
                        break;
                    case "week":
                        logDateValueType = "WW \"week\"";
                        break;
                }
            }

            if (req.GroupByOneIsTime == false && req.GroupBy1 != null && req.GroupBy1.ToLower() == "resourcename")
            {
                sumStartPartQuery =
                    $"\"ResourceName\",  TO_CHAR(TO_TIMESTAMP(\"{logDate}\" /1000) AT TIME ZONE 'IST', \'{logDateValueType}\') as day,";
            }
            else if (req.GroupByOneIsTime || req.GroupByTwoIsTime)
            {
                sumStartPartQuery =
                    $"\"{propertyNameColumn}\", TO_CHAR(TO_TIMESTAMP(\"{logDate}\" /1000) AT TIME ZONE 'IST', \'{logDateValueType}\') as day,";
            }
            else
            {
                sumStartPartQuery = $"\"{propertyNameColumn}\", ";
            }

            if (forAverage)
            {
                sumStartPartQuery += $"Round(AVG(CAST(\"NewValue\" AS float))::numeric, 2) as total";
            }
            else
            {
                sumStartPartQuery += $"Round(SUM(CAST(\"NewValue\" AS float))::numeric, 2) as total";
            }

            whereClausePart += $" Where (";
            foreach (KeyValuePair<string, PropertyType> entry in req.FieldName)
            {
                whereClausePart += $"\"{propertyNameColumn}\"=\'{entry.Key}\'";
                if (!req.FieldName[entry.Key].Equals(req.FieldName.Last().Key))
                {
                    whereClausePart += "or ";
                }
            }

            whereClausePart = whereClausePart.Substring(0, whereClausePart.Length - 3);
            whereClausePart += ") and \"NewValue\" != '' ";
            whereClausePart +=
                $"and TO_TIMESTAMP(\'{startTime}\', 'YYYY-MM-DD HH24:MI:SS') <= TO_TIMESTAMP(\"{logDate}\" / 1000)";
            whereClausePart +=
                $"and TO_TIMESTAMP(\'{endTime}\', 'YYYY-MM-DD HH24:MI:SS') >= TO_TIMESTAMP(\"{logDate}\" / 1000)";

            whereClausePart = addBaseorPropertyFilterQuery(req, whereClausePart);
            if (req.GroupByOneIsTime == false && req.GroupBy1 != null && req.GroupBy1.ToLower() == "resourcename")
            {
                whereClausePart += $"Group by day,  \"ResourceName\"";
                whereClausePart += "order by day";
            }
            else if (req.GroupByOneIsTime || req.GroupByTwoIsTime)
            {
                whereClausePart += $"Group by day, \"{propertyNameColumn}\" ";
                whereClausePart += "order by day";
            }
            else
            {
                whereClausePart += $"Group by \"{propertyNameColumn}\" ";
            }
        }

        private static void QueryForAverage(WidgetRequestModel req, string whereClause, out string StartPartQuery,
            out string EndPartQuery, out string GroupByOptionalPart, out string whereClausePart)
        {
            StartPartQuery = "";
            EndPartQuery = "";
            GroupByOptionalPart = "";
            whereClausePart = "";

            QueryForAverageForEventsAndResource(req, whereClause, out StartPartQuery, out EndPartQuery,
                out GroupByOptionalPart, out whereClausePart);
        }

        private static void QueryForAverageForEventsAndResource(WidgetRequestModel req, string whereClause,
            out string StartPartQuery, out string EndPartQuery, out string GroupByOptionalPart,
            out string whereClausePart)
        {
            whereClausePart = whereClause;
            string columnName = string.Empty;
            string previousColumnName = string.Empty;
            StartPartQuery = string.Empty;
            EndPartQuery = string.Empty;
            GroupByOptionalPart = string.Empty;
            foreach (var it in req.FieldName.Select((name, Index) => new { name, Index }))
            {
                var fieldName = it.name;
               
                    if (fieldName.Value == PropertyType.Number || fieldName.Value == PropertyType.Float)
                    {
                        if (string.IsNullOrEmpty(columnName))
                        {
                            columnName = fieldName.Key;
                            StartPartQuery = $"  Round(avg(columnName),2) avg ";
                            EndPartQuery =
                                $" ,jsonb_each_text(\"_Properties\") as pair(key,value) where  key ~* '({columnName})'";
                            GroupByOptionalPart = " Group By ";
                            previousColumnName = columnName;
                        }
                        else
                        {
                            columnName = fieldName.Key.Split('.')[1];
                            string newColumnValue = $"{previousColumnName}|{columnName}";
                            EndPartQuery = EndPartQuery.Replace(previousColumnName, newColumnValue);
                            previousColumnName = newColumnValue;
                        }

                        if (req.IsDistinct && !StartPartQuery.Contains("key,"))
                        {
                            StartPartQuery = $" key, {StartPartQuery}";
                            GroupByOptionalPart = " Group By key";
                        }
                    }
                
            }
        }

        private static void QueryForSum(WidgetRequestModel req, out string aggregateSumQueryStartingPart,
            out string aggregateSumQueryEndingPart, out string sumGroupByOptionalPart)
        {
            aggregateSumQueryStartingPart = string.Empty;
            aggregateSumQueryEndingPart = string.Empty;
            var columnsToClubQuery = string.Empty;
            sumGroupByOptionalPart = string.Empty;
            if (req.ClubbingFieldName != "" && req.FieldName.Count > 0)
            {
                columnsToClubQuery = "Sum(";
                sumGroupByOptionalPart = " Group By ";
            }

            foreach (var it in req.FieldName.Select((name, Index) => new { name, Index }))
            {
                var fieldName = it.name;
                var columnQuery = "";
                if (fieldName.Value == PropertyType.Number || fieldName.Value == PropertyType.Float)
                {
                    columnQuery += $"Sum({fieldName.Key}) as {fieldName.Key}";
                }

                aggregateSumQueryStartingPart += columnQuery;
                if (req.ClubbingFieldName != "" && req.FieldName.Count > 0)
                {
                    columnsToClubQuery += fieldName.Key;
                }

                if (it.Index < req.FieldName.Count)
                {
                    aggregateSumQueryStartingPart += ',';
                }
                if (it.Index < req.FieldName.Count-1)
                {
                    columnsToClubQuery += "+";
                }
            }

            if (req.ClubbingFieldName != "" && req.FieldName.Count > 0)
            {
                columnsToClubQuery += $") as {req.ClubbingFieldName}";
                aggregateSumQueryStartingPart += columnsToClubQuery;
            }

            aggregateSumQueryEndingPart = "";
        }


        private static void QueryForGroupByTime(string groupByTimeName, out string groupByTimeStartPart, out string groupByTimeMidPart,
            out string groupByTimeEndPart)
        {
            string mapping;
            TimePeriodToItsClubMapping.TryGetValue(groupByTimeName, out mapping);
            groupByTimeStartPart = $"{groupByTimeName}";
            groupByTimeMidPart = $"date_trunc('{mapping}', TO_TIMESTAMP(\"Time\")) as {groupByTimeName}";
            groupByTimeEndPart = $"{groupByTimeName}";
        }

        private static void QueryForGroupByProperty(string GroupByPropertyName, out string groupByPropertyPart1,
            out string groupByPropertyPart2)
        {
            string GroupByColumn;
            GroupByColumn = GroupByPropertyName;
            groupByPropertyPart1 = $"\"{GroupByColumn}\"";
            groupByPropertyPart2 = $"\"{GroupByColumn}\"";
        }

        private static string JoinQuery(WidgetRequestModel req, Dictionary<string, string> queryPartNameToQueryMap)
        {
            string finalQuery = string.Empty;
            string basequery = string.Empty;
            string whereClausePart = string.Empty;

            queryPartNameToQueryMap.TryGetValue("baseQuery", out basequery);
            queryPartNameToQueryMap.TryGetValue("whereClausePart", out whereClausePart);

            if (req.FieldName != null && req.FieldName.Count > 0)
            {
                string aggregateMethodStartingPartQuery = string.Empty,
                    aggregateMethodEndPartQuery = string.Empty,
                    aggregateMethodGroupByOptionalPart = string.Empty;
                queryPartNameToQueryMap.TryGetValue("aggregateMethodStartingPartQuery", out aggregateMethodStartingPartQuery);
                queryPartNameToQueryMap.TryGetValue("aggregateMethodEndPartQuery", out aggregateMethodEndPartQuery);
                queryPartNameToQueryMap.TryGetValue("aggregateMethodGroupByOptionalPart", out aggregateMethodGroupByOptionalPart);

                if (req.Method == Enum_Method.Count)
                {
                    if (!string.IsNullOrEmpty(whereClausePart))
                    {
                        basequery += $" WHERE {whereClausePart}";
                    }
                }

                basequery = basequery.Replace("*", aggregateMethodStartingPartQuery);
                basequery += aggregateMethodEndPartQuery;

                if (req.Method == Enum_Method.Sum || req.Method == Enum_Method.Average)
                {
                    basequery += aggregateMethodGroupByOptionalPart;
                }

                JoinQueryGroup2Handling(req, ref queryPartNameToQueryMap);

                if (!string.IsNullOrEmpty(req.GroupBy1) && req.Entity != Enum_Entity.Events)
                {
                    finalQuery = JoinQueryGroup1Handling(req, ref basequery, ref queryPartNameToQueryMap);
                }
                else
                {
                    // handling distinct
                    if (req.IsDistinct)
                    {
                        if (req.Method == Enum_Method.Count)
                        {
                            basequery = basequery.Replace("as subpart1", "");
                        }
                        else
                        {
                            basequery = basequery.Replace("Group By", $" AND {whereClausePart} Group By");
                            // because in sum/Avg id property is EventsPrperties we use jsonb_each_text and it should
                            // appear before whereclause
                        }

                        finalQuery = basequery;
                    }
                    else // handling single
                    {
                        if (req.Method == Enum_Method.Count)
                        {
                            finalQuery = $"Select sum(subpart1.count) From ({basequery}) as subpart1";
                        }
                        else // for sum/avg
                        {
                            //if (!string.IsNullOrEmpty(req.GroupBy1))
                            //{
                            //    basequery = basequery.Replace("Group By", $" AND {whereClausePart} Group By");
                            //}
                            //else
                            //


                            basequery = basequery.Replace("Group By", $" AND {whereClausePart} ");

                            //}
                            finalQuery = basequery;
                        }
                    }

                    if (req.Entity == Enum_Entity.Events)
                    {
                        basequery += whereClausePart;
                        finalQuery = basequery;
                    }
                }
            }
            else
            {
                finalQuery = $"{basequery} WHERE {whereClausePart}";
            }

            // if (req.Pagination || req.PageLimit > 0)
            // {
            //     var skip = (req.PageNumber);
            //     if (skip < 0 || req.PageNumber == 1)
            //     {
            //         skip = 0;
            //     }
            //
            //     var query = "";
            //     if (!string.IsNullOrEmpty(whereClausePart))
            //     {
            //         query = $"{basequery} WHERE {whereClausePart};";
            //     }
            //     else
            //     {
            //         query = $"{basequery};";
            //     }
            //
            //     finalQuery = query.Replace("*", "Count(*)");
            //     if (!string.IsNullOrEmpty(whereClausePart))
            //     {
            //         finalQuery += $"{basequery} WHERE {whereClausePart}";
            //     }
            //     else
            //     {
            //         finalQuery += $"{basequery}";
            //     }
            //
            //     if (req.WidgetType == Enum_WidgetType.Table && req.Entity == Enum_Entity.Events)
            //     {
            //         string whereclauseForCount =
            //             $"WHERE \"Log_Date\" >= EXTRACT(epoch FROM TIMESTAMP '{req.StartTime}') * 1000 AND \"Log_Date\" <= EXTRACT(epoch FROM TIMESTAMP '{req.EndTime}') * 1000;";
            //         finalQuery.Replace(";", whereclauseForCount);
            //         finalQuery += whereclauseForCount;
            //     }
            //
            //     finalQuery += $" ORDER BY \"Id\" DESC OFFSET ({skip}) ROWS FETCH NEXT ({req.PageLimit}) ROWS ONLY ";
            // }

            // replace double space by single space
            
            finalQuery = finalQuery.Replace("  ", " ");
            finalQuery = finalQuery.Trim();
            return finalQuery;
        }

        private static void PropertyGetColumnName(string fieldName, out string columnName)
        {
            if (!fieldName.Contains("EventsProperties"))
            {
                columnName = fieldName;
            }
            else
            {
                columnName = fieldName.Split('.')[1];
            }
        }

        private static void JoinQueryGroup2Handling(WidgetRequestModel req,
            ref Dictionary<string, string> queryPartNameToQueryMap)
        {
            if (!string.IsNullOrEmpty(req.GroupBy2) && req.Entity != Enum_Entity.Events)
            {
                if (req.GroupByTwoIsTime)
                {
                    string groupBy2TimeMidPart = string.Empty, groupBy2TimeEndPart = string.Empty;
                    queryPartNameToQueryMap.TryGetValue("groupBy2TimeMidPart", out groupBy2TimeMidPart);
                    queryPartNameToQueryMap.TryGetValue("groupBy2TimeEndPart", out groupBy2TimeEndPart);

                    string groupBy1PropertyPart1 = string.Empty, groupBy1PropertyPart2 = string.Empty;
                    queryPartNameToQueryMap.TryGetValue("groupBy1PropertyPart1", out groupBy1PropertyPart1);
                    queryPartNameToQueryMap.TryGetValue("groupBy1PropertyPart2", out groupBy1PropertyPart2);

                    groupBy1PropertyPart1 += $",{groupBy2TimeMidPart}";
                    groupBy1PropertyPart2 += $",{groupBy2TimeEndPart}";

                    queryPartNameToQueryMap["groupBy1PropertyPart1"] = groupBy1PropertyPart1;
                    queryPartNameToQueryMap["groupBy1PropertyPart2"] = groupBy1PropertyPart2;
                }
                else
                {
                    string groupBy1PropertyPart1 = string.Empty, groupBy1PropertyPart2 = string.Empty;
                    queryPartNameToQueryMap.TryGetValue("groupBy1PropertyPart1", out groupBy1PropertyPart1);
                    queryPartNameToQueryMap.TryGetValue("groupBy1PropertyPart2", out groupBy1PropertyPart2);

                    string groupBy2PropertyPart1 = string.Empty, groupBy2PropertyPart2 = string.Empty;
                    queryPartNameToQueryMap.TryGetValue("groupBy2PropertyPart1", out groupBy2PropertyPart1);
                    queryPartNameToQueryMap.TryGetValue("groupBy2PropertyPart2", out groupBy2PropertyPart2);

                    if (!string.IsNullOrEmpty(groupBy1PropertyPart1))
                    {
                        groupBy1PropertyPart1 += $",";
                    }

                    groupBy1PropertyPart1 += $"{groupBy2PropertyPart1}";
                    if (!string.IsNullOrEmpty(groupBy1PropertyPart2))
                    {
                        groupBy1PropertyPart2 += $",";
                    }
                    groupBy1PropertyPart2 += $"{groupBy2PropertyPart2}";
                    queryPartNameToQueryMap["groupBy1PropertyPart1"] = groupBy1PropertyPart1;
                    queryPartNameToQueryMap["groupBy1PropertyPart2"] = groupBy1PropertyPart2;
                }
            }
        }
        
        private static string JoinQueryGroup1Handling(WidgetRequestModel req, ref string basequery,
            ref Dictionary<string, string> queryPartNameToQueryMap)
        {
            string finalQuery = string.Empty;
            string whereClausePart = string.Empty;
            queryPartNameToQueryMap.TryGetValue("whereClausePart", out whereClausePart);

            if (req.GroupByOneIsTime)
            {
                string groupBy1TimeStartPart = string.Empty, groupBy1TimeMidPart = string.Empty, groupBy1TimeEndPart = string.Empty;
                queryPartNameToQueryMap.TryGetValue("groupBy1TimeStartPart", out groupBy1TimeStartPart);
                queryPartNameToQueryMap.TryGetValue("groupBy1TimeMidPart", out groupBy1TimeMidPart);
                queryPartNameToQueryMap.TryGetValue("groupBy1TimeEndPart", out groupBy1TimeEndPart);

                basequery = basequery.Replace("SELECT", $"(SELECT {groupBy1TimeMidPart}, ");
                basequery += $",{groupBy1TimeEndPart} ) ";
            }
            else
            {
                string groupBy1PropertyPart1 = string.Empty, groupBy1PropertyPart2 = string.Empty;
                queryPartNameToQueryMap.TryGetValue("groupBy1PropertyPart1", out groupBy1PropertyPart1);
                queryPartNameToQueryMap.TryGetValue("groupBy1PropertyPart2", out groupBy1PropertyPart2);

                basequery = basequery.Replace("SELECT", $"(SELECT {groupBy1PropertyPart1}, ");
                basequery += $",{groupBy1PropertyPart2} ) ";
            }
            

            if (req.Method == Enum_Method.Count || !string.IsNullOrEmpty(req.GroupBy1))
            {
                basequery += "as subpart1 ";
                // in case of count it is always needed but in sum/avg only needed when time clubbing is true
                // since to we need to prepend To_Char in that query
            }

            string GroupByColumn = string.Empty;
            string columnName = string.Empty;
            // groupby1 time handling
            if (req.GroupByOneIsTime)
            {
                string groupBy1TimeStartPart = string.Empty;
                queryPartNameToQueryMap.TryGetValue("groupBy1TimeStartPart", out groupBy1TimeStartPart);
                if (req.ClubbingTime)
                {
                    GroupByColumn = $"{groupBy1TimeStartPart}";
                }
                else
                {
                    GroupByColumn = $"{groupBy1TimeStartPart}";
                }
            }
            else
            {
                // groupby1 property other than time
                PropertyGetColumnName(req.GroupBy1, out GroupByColumn);
                GroupByColumn = req.GroupBy1;
            }

            if (req.Method == Enum_Method.Count)
            {
                // in case of count property can be a one single string
                PropertyGetColumnName(req.FieldName.First().Key, out columnName);
                columnName = req.FieldName.First().Key;
            }
            else
            {   
                // in case of sum/avg there can be multiple properties eg. car,bus,truck
                foreach (var field in req.FieldName)
                {
                    // if (!field.Key.Contains("eventProperties"))
                    // {
                    //     string temp;
                    //     PropertyGetColumnName(field.Key, out temp);
                    //     if (string.IsNullOrEmpty(columnName))
                    //     {
                    //         columnName = $" subpart1.{temp}";
                    //     }
                    //     else
                    //     {
                    //         columnName += $",subpart1.{temp}";
                    //     }
                    // }
                    columnName += $",subpart1.{field.Key}";
                }
            }

            string GroupBy2Column = string.Empty;
            // adding group 2 part in final query for both time/property cases
            if (!string.IsNullOrEmpty(req.GroupBy2))
            {
                if (req.GroupByTwoIsTime)
                {
                    string groupBy2TimeStartPart = string.Empty;
                    queryPartNameToQueryMap.TryGetValue("groupBy2TimeStartPart", out groupBy2TimeStartPart);
                    if (req.ClubbingTime)
                    {
                        GroupBy2Column = $", {groupBy2TimeStartPart}";
                    }
                    else
                    {
                        GroupBy2Column = $", subpart1.{groupBy2TimeStartPart}";
                    }

                }
                else
                {
                    PropertyGetColumnName(req.GroupBy2, out GroupBy2Column);
                    GroupBy2Column = req.GroupBy2;
                    GroupBy2Column = $", subpart1.{GroupBy2Column}"; // TODO: check this 2 count queries failed after removing quotes around this
                }
            }

            basequery = AddOrderBy(basequery);
            
            if (req.Method == Enum_Method.Count)
            {
                finalQuery = CountFinalQueryHandling(req, basequery, GroupByColumn, GroupBy2Column, columnName);
            }
            else if(req.Method== Enum_Method.Sum)
            {
                finalQuery = SumAvgFinalQueryHandling(req, basequery, GroupByColumn, GroupBy2Column, whereClausePart,columnName,"sum");
            }
            else
            {
                finalQuery = SumAvgFinalQueryHandling(req, basequery, GroupByColumn, GroupBy2Column, whereClausePart, columnName, "avg");
            }

            return finalQuery;
        }

        private static string CountFinalQueryHandling(WidgetRequestModel req, string basequery,
            string GroupByColumn,
            string GroupBy2Column, string columnName)
        {
            string finalQuery = string.Empty;
            if (req.IsDistinct)
            {
                finalQuery =
                    $"Select subpart1.{GroupByColumn}{GroupBy2Column},subpart1.{columnName},sum(subpart1.count) as count From {basequery} group by subpart1.{GroupByColumn},subpart1.{columnName}{GroupBy2Column}";
            }
            else
            {
                finalQuery =
                    $"Select subpart1.{GroupByColumn}{GroupBy2Column},sum(subpart1.count) as count From {basequery} group by subpart1.{GroupByColumn}{GroupBy2Column}";
            }

            // for club time you don't need any part from sub query which is denoted as subpart2 
            // that's why we have to remove subquery name from front of TimeClubbing function i.e TO_CHAR()
            if (req.GroupByOneIsTime && req.ClubbingTime)
            {
                finalQuery = finalQuery.Replace($"subpart1.{GroupByColumn}", $"{GroupByColumn}");
            }

            if (req.GroupByTwoIsTime && req.ClubbingTime)
            {
                finalQuery = finalQuery.Replace($"subpart1.{GroupBy2Column}", $"\"{GroupBy2Column}\"");
            }

            return finalQuery;
        }

        public static string AddOrderBy(string sqlQuery)
        {
            // Define the regex pattern to match the GROUP BY clause
            string pattern = @"Group By (.*?)(?=\))";

            // Extract the GROUP BY clause using regex
            Match match = Regex.Match(sqlQuery, pattern);
            if (match.Success)
            {
                // Split the GROUP BY entities
                string[] groupByEntities = match.Groups[1].Value.Split(',');

                string groupByPart = match.ToString();
                // Reconstruct the GROUP BY clause
                string orderByClausePart = "Order By " + string.Join(",", groupByEntities);
                string newPart = $"{groupByPart} {orderByClausePart}";
                // Replace the original GROUP BY clause with the sorted one
                string sortedSqlQuery = sqlQuery.Replace(groupByPart, newPart);

                // Add ORDER BY clause within the subquery
                // sortedSqlQuery = sortedSqlQuery.Insert(sortedSqlQuery.IndexOf(")"), " ORDER BY " + string.Join(",", groupByEntities));

                return sortedSqlQuery;
            }

            return sqlQuery;
        }
        private static string SumAvgFinalQueryHandling(WidgetRequestModel req, string basequery,
            string GroupByColumn,
            string GroupBy2Column, string whereClausePart, string columnName, string aggeregrateType)
        {
            string finalQuery = string.Empty;
            if (req.IsDistinct)
            {
                basequery = basequery.Replace("Group By", $" AND {whereClausePart} Group By");
                if (!basequery.Contains("where"))
                {
                    basequery = Regex.Replace(basequery, "(Group\\sBy\\s*,)", "Group By ");
                    basequery = Regex.Replace(basequery, "(Order\\sBy\\s*,)", "Order By ");
                    basequery = basequery.Replace($" AND {whereClausePart} Group By",
                        $" where {whereClausePart} Group By");
                }

                    finalQuery =
                        $"Select subpart1.{GroupByColumn}{GroupBy2Column},Round({aggeregrateType}(subpart1.{aggeregrateType}),2) as {aggeregrateType} From {basequery} group by subpart1.{GroupByColumn}{GroupBy2Column} order by subpart1.{GroupByColumn}{GroupBy2Column}";

                    if (!req.ClubbingTime)
                    {
                        basequery += 
                        finalQuery = basequery.Replace(" as subpart1", " ");
                    }
                    else
                    {
                        finalQuery =
                            $"Select subpart1.{GroupByColumn}{GroupBy2Column},{columnName} From {basequery} group by subpart1.{GroupByColumn}{GroupBy2Column},{columnName}  order by subpart1.{{GroupByColumn}}{{GroupBy2Column}},{{columnName}}";
                    }
                
            }
            else
            {
                basequery = Regex.Replace(basequery, "(Group\\sBy\\s*,)", "Group By ");
                basequery = basequery.Replace("Group By ", $" AND {whereClausePart} Group By ");
                if (!basequery.Contains("where"))
                {
                    basequery = basequery.Replace($" AND {whereClausePart} Group By ",
                        $" where {whereClausePart} Group By ");
                }


                    finalQuery =
                        $"Select subpart1.{GroupByColumn}{GroupBy2Column},Round({aggeregrateType}(subpart1.{aggeregrateType}),2) as {aggeregrateType} From {basequery} group by subpart1.{GroupByColumn}{GroupBy2Column} order by subpart1.{GroupByColumn}{GroupBy2Column}";
                

                    if (!req.ClubbingTime)
                    {
                        finalQuery = basequery.Replace(" as subpart1", " ");
                    }
                    else
                    {
                        finalQuery =
                            $"Select subpart1.{GroupByColumn}{GroupBy2Column},subpart1.{aggeregrateType} From {basequery} group by subpart1.{GroupByColumn}{GroupBy2Column},subpart1.{aggeregrateType} order by subpart1.{GroupByColumn}{GroupBy2Column},subpart1.{aggeregrateType}";
                    }
                
            }


            // for club time you don't need any part from sub query which is denoted as subpart2 
            // that's why we have to remove subquery name from front of TimeClubbing function i.e TO_CHAR()
            if (req.GroupByOneIsTime && req.ClubbingTime)
            {
                finalQuery = finalQuery.Replace($"subpart1.{GroupByColumn}", $"{GroupByColumn}");
            }

            if (req.GroupByTwoIsTime && req.ClubbingTime)
            {
                finalQuery = finalQuery.Replace($"subpart1.{GroupBy2Column}", $"{GroupBy2Column}");
            }

            return finalQuery;
        }

        public static DateTime UnixTimeStampToDateTime(long unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            DateTime dateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            dateTime = dateTime.AddMilliseconds(unixTimeStamp).ToLocalTime();
            return dateTime;
        }
    }
}