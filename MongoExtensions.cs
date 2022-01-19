using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Collections;
using Newtonsoft.Json.Linq;
using System;
using System.Reflection;
using MongoDB.Bson.Serialization.Attributes;
using System.Linq.Expressions;
using CSharpExtensions.OpenSource;

namespace CSharpExtensions.OpenSource.Mongo
{
    public static class MongoExtensions
    {
        public static IMongoCollection<T> GetMongoCollection<T>(this string connectionString, string dbName, string collectionName)
        {
            IMongoClient client = new MongoClient(connectionString);
            IMongoDatabase database = client.GetDatabase(dbName);
            return database.GetCollection<T>(collectionName);
        }

        private static Expression<Func<T, object>> PropToLambda<T>(string propName)
        {
            var paramExpression = Expression.Parameter(typeof(T));
            var propExpression = Expression.PropertyOrField(paramExpression, propName);
            var propertyObjExpr = Expression.Convert(propExpression, typeof(object));
            var lamda = Expression.Lambda<Func<T, object>>(propertyObjExpr, paramExpression)!;
            return lamda;
        }

        public static string? RenderToJson<T>(this FilterDefinition<T> filter)
        {
            var serializerRegistry = MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry;
            var documentSerializer = serializerRegistry.GetSerializer<T>();
            return filter.Render(documentSerializer, serializerRegistry).BsonToJson();
        }
        private static UpdateDefinition<T> SetOrSetOnInsertAllProps<T>(this UpdateDefinitionBuilder<T> updateBuilder, bool setOnInsert, T obj, bool setIfNotEmpty = false, bool setIfNotNull = false, params string[] excludeProps) where T : class
        {
            var props = obj.GetPropsToUpdate(setIfNotEmpty, setIfNotNull, excludeProps);
            var update = updateBuilder.Unset(" ");
            foreach (var prop in props)
            {
                var lamda = PropToLambda<T>(prop.Name);
                var val = lamda.Compile()(obj);
                // regular prop
                if (prop.GetSetMethod() != null)
                {
                    update = setOnInsert ? update.SetOnInsert(lamda, val) : update.Set(lamda, val);
                }
                // prop with only getter
                else
                {
                    update = setOnInsert ? update.SetOnInsert(prop.Name, val) : update.Set(prop.Name, val);
                }
            }
            return update;
        }

        public static UpdateDefinition<T> SetOnInsertAllProps<T>(this UpdateDefinitionBuilder<T> updateBuilder, T obj, bool setIfNotEmpty = false, bool setIfNotNull = false, params string[] excludeProps) where T : class
            => SetOrSetOnInsertAllProps(updateBuilder, setOnInsert: true, obj, setIfNotEmpty, setIfNotNull, excludeProps);

        public static UpdateDefinition<T> SetAllProps<T>(this UpdateDefinitionBuilder<T> updateBuilder, T obj, bool setIfNotEmpty = false, bool setIfNotNull = false, params string[] excludeProps) where T : class
            => SetOrSetOnInsertAllProps(updateBuilder, setOnInsert: false, obj, setIfNotEmpty, setIfNotNull, excludeProps);

        public static List<PropertyInfo> GetPropsToUpdate<T>(this T obj, bool setIfNotEmpty = false, bool setIfNotNull = false, params string[] excludeProps) where T : class
        {
            var props = obj.GetType().PowerfulGetProperties()
                .Where(x => !excludeProps.Any(excludeName => x.Name == excludeName))
                .Where(x => x.GetCustomAttribute(typeof(BsonIgnoreAttribute), true) == null)
                .ToList();
            var res = new List<PropertyInfo>();
            foreach (var prop in props)
            {
                var val = prop.GetValue(obj);
                var shouldIgnoreIfNull = setIfNotNull || prop.GetCustomAttribute(typeof(BsonIgnoreAttribute), true) != null;
                var shouldIgnoreIfDefault = prop.GetCustomAttribute(typeof(BsonIgnoreIfDefaultAttribute), true) != null;
                if (val == null && shouldIgnoreIfNull)
                {
                    continue;
                }
                if ((val == null || (val is string str && string.IsNullOrEmpty(str.Trim()))) && setIfNotEmpty)
                {
                    continue;
                }
                if (shouldIgnoreIfDefault && val == default)
                {
                    continue;
                }
                res.Add(prop);
            }
            return res.DistinctBy(x => x.Name).ToList();
        }

        public static async Task RemoveDuplicatesAsync<T, S>(this IMongoCollection<T> mongoCollection, Expression<Func<T, S>> uniqueKey, Expression<Func<T, ObjectId?>> mongoId, CancellationToken ct = default)
        {
            await foreach (var bulk in mongoCollection.Pagination(Builders<T>.Filter.Empty, Builders<T>.Sort.Descending("_id"), 1000))
            {
                if (ct.IsCancellationRequested) { return; }
                var bulkDel = new List<DeleteManyModel<T>>();
                foreach (var item in bulk)
                {
                    bulkDel.Add(new(Builders<T>.Filter.And(
                        Builders<T>.Filter.Eq(uniqueKey, uniqueKey.Compile()(item)),
                        Builders<T>.Filter.Lt(mongoId, mongoId.Compile()(item))
                    )));
                }
                if (bulkDel.Any()) { await mongoCollection.BulkWriteAsync(bulkDel, new() { IsOrdered = true }); }
            }
        }

        public static async Task<BulkWriteResult<T>?> EnumsNumbersToStrings<T>(this IMongoCollection<T> col, params Expression<Func<T, Enum?>>[] propsExp)
        {
            var bulk = new List<UpdateManyModel<T>>();
            foreach (var propExp in propsExp)
            {
                var expression = (propExp.Body as MemberExpression ?? ((UnaryExpression)propExp.Body).Operand as MemberExpression) ?? throw new Exception($"Expression '{propExp}' not supported.");
                var propName = expression.Member.Name;
                var propType = (expression.Member as PropertyInfo)?.PropertyType ?? (expression.Member as FieldInfo)?.FieldType ?? throw new Exception($"Expression Member '{expression.Member}' not supported.");
                propType = Nullable.GetUnderlyingType(propType) ?? propType;
                var lamda = PropToLambda<T>(propName);
                foreach (var item in Enum.GetValues(propType))
                {
                    var invVal = (int)item;
                    var stringVal = item.ToString()!;
                    var filter = Builders<T>.Filter.Eq(lamda, invVal);
                    var update = Builders<T>.Update.Set(lamda, stringVal);
                    bulk.Add(new(filter, update));
                }
            }
            if (bulk.Any()) { return await col.BulkWriteAsync(bulk, new() { IsOrdered = true }); }
            return null;
        }

        public static async IAsyncEnumerable<List<T>> Pagination<T>
        (
            this IMongoCollection<T> col,
            FilterDefinition<T> filter,
            SortDefinition<T> sort,
            int size = 1000,
            bool disableSkip = false,
            bool updateTotalCount = false,
            bool disableCount = false,
            Action<string>? logger = null
        )
        {
            logger = logger ?? (str => { });
            var totalCount = disableCount ? -1 : await col.CountDocumentsAsync(filter);
            var index = 0;
            logger($"MongoPagination - {col.CollectionNamespace.CollectionName} - {(totalCount > 0 ? $"{totalCount} items, " : "")}{size} per get");
            var totalExec = 0;
            while (true)
            {
                var data = await col.Find(filter)
                    .Sort(sort)
                    .Skip(disableSkip ? 0 : index * size)
                    .Limit(size)
                    .ToListAsync();
                if (!data.Any()) { yield break; }
                totalExec += data.Count;
                if (!disableCount && updateTotalCount) { totalCount = await col.CountDocumentsAsync(filter); }
                logger($"MongoPagination - {col.CollectionNamespace.CollectionName} - {totalExec}{(totalCount > 0 ? $"/{totalCount}" : "")}");
                yield return data;
                index++;
            }
        }

        public static async IAsyncEnumerable<T> PaginationWithUpdate<T>
        (
            this IMongoCollection<T> col,
            FilterDefinition<T> filter,
            UpdateDefinition<T> update,
            SortDefinition<T>? sort = null,
            Action<string>? logger = null
        )
        {
            logger = logger ?? (str => { });
            var totalCount = await col.CountDocumentsAsync(filter);
            logger($"MongoPaginationWithUpdate - {col.CollectionNamespace.CollectionName} - {totalCount} items");
            var totalExec = 0;
            var lastTotalSync = DateTime.UtcNow;
            while (true)
            {
                var data = await col.FindOneAndUpdateAsync(filter, update, new FindOneAndUpdateOptions<T, T> { Sort = sort });
                if (data != null)
                {
                    yield return data;
                }
                totalExec++;
                if ((DateTime.UtcNow - lastTotalSync).TotalMinutes >= 1)
                {
                    totalCount = await col.CountDocumentsAsync(filter);
                    lastTotalSync = DateTime.UtcNow;
                }
                logger($"MongoPaginationWithUpdate - {col.CollectionNamespace.CollectionName} - {totalExec}/{totalCount}");
            }
        }

        public static async Task<List<T>> GetAllByBulks<T>(this IMongoCollection<T> col, FilterDefinition<T> filter, SortDefinition<T> sort, int size = 1000, bool disableSkip = false)
        {
            var res = new List<T>();
            await foreach (var bulk in col.Pagination(filter, sort, size, disableSkip))
            {
                res.AddRange(bulk);
            }
            return res;
        }

        public static PipelineDefinition<T, S> GetPipelineDefinition<T, S>(this string pipelineJson)
        {
            var bsonDocArray = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument[]>(pipelineJson);
            var pipelineDefinition = PipelineDefinition<T, S>.Create(bsonDocArray);
            return pipelineDefinition;
        }

        public static Task<IAsyncCursor<BsonDocument>> AggregateAsync<T>(this IMongoCollection<T> col, string aggQuery, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        {
            var pipelineDefinition = aggQuery.GetPipelineDefinition<T, BsonDocument>();
            return col.AggregateAsync(pipelineDefinition, options, cancellationToken);
        }

        public static IAsyncCursor<BsonDocument> Aggregate<T>(this IMongoCollection<T> col, string aggQuery, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        {
            var bsonArray = BsonArray.Create(aggQuery);
            var bsonDocArray = bsonArray.Select(x => x.AsBsonDocument).ToArray();
            var pipelineDefinition = PipelineDefinition<T, BsonDocument>.Create(bsonDocArray);
            return col.Aggregate(pipelineDefinition, options, cancellationToken);
        }
        public static Task<List<T>> GetAllAsync<T>(this IMongoCollection<T> collection) => collection.Find(item => true).ToListAsync();

        public static dynamic? ToDynamic<T>(this T? value)
        {
            if (value == null) { return null; }
            var jsonSettings = GenericsExtensions.GetJsonSerializerSettings(TypeNameHandling.None);
            string? json = "";
            if (value is BsonDocument)
            {
                var jsonWriterSettings = new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.Strict };
                json = value.ToJson(jsonWriterSettings);
            }
            else
            {
                json = value is string ? value.ToString() : JsonConvert.SerializeObject(value, jsonSettings);
            }
            if (json != null && json[0] == '[')
            {
                try
                {
                    return JsonConvert.DeserializeObject<ExpandoObject[]>(json, jsonSettings)!;
                }
                catch
                {
                    return JsonConvert.DeserializeObject(json, jsonSettings)!;
                }
            }
            try
            {
                return JsonConvert.DeserializeObject<ExpandoObject>(json ?? "{}", jsonSettings)!;
            }
            catch
            {
                return JsonConvert.DeserializeObject(json ?? "{}", jsonSettings)!;
            }
        }

        public static BsonValue ToBsonValue<T>(this T value)
        {
            var jsonSettings = GenericsExtensions.GetJsonSerializerSettings(TypeNameHandling.None);
            if (!(value is ExpandoObject) && value is IEnumerable collection)
            {
                var arr = new BsonArray();
                foreach (var item in collection)
                {
                    arr.Add(ToBsonValue(item));
                }
                return arr;
            }
            var json = JToken.Parse(JsonConvert.SerializeObject(value, jsonSettings)).RemovePropRecursive("$type").ToString();
            return BsonDocument.Parse(json);
        }
        public static async Task<List<string>> GetIndexesNames<T>(this IMongoCollection<T> col) => (await (await col.Indexes.ListAsync()).ToListAsync()).Select(x => x["name"].AsString).ToList();

        public static async Task CreateIndexesIfNotExists<T>
        (
            this IMongoCollection<T> col,
            List<CreateIndexModel<T>> createIndexModels,
            bool forceUpdate = false,
            bool dropIndexesThatNotInTheList = false
        )
        {
            var names = createIndexModels.Select(x => x.Options.Name);
            if (names.Any(x => string.IsNullOrEmpty(x)))
            {
                throw new Exception("Must Set CreateIndexModel -> Option -> Name");
            }
            if (dropIndexesThatNotInTheList)
            {
                var allIndexes = await col.GetIndexesNames();
                foreach (var index in allIndexes.Where(x => x != "_id" && x != "_id_"))
                {
                    if (!names.Contains(index))
                    {
                        Console.WriteLine($"{col.CollectionNamespace.CollectionName} - Droping Index {index}");
                        await col.Indexes.DropOneAsync(index);
                    }
                }
            }
            var existingIndexes = await col.GetIndexesNames();
            foreach (var createIndexModel in createIndexModels)
            {
                if (!forceUpdate && existingIndexes.Contains(createIndexModel.Options.Name))
                {
                    continue;
                }
                try
                {
                    await col.Indexes.CreateOneAsync(createIndexModel);
                }
                catch (Exception ex) when (ex.ToString().Contains("Index must have unique name"))
                {
                    await col.Indexes.DropOneAsync(createIndexModel.Options.Name);
                    await col.Indexes.CreateOneAsync(createIndexModel);
                }
            }
        }

        public static T CleanupBson<T>(this T value)
        {
            var json = value.BsonToJson().ToCleanJson();
            var bson = MongoDB.Bson.BsonDocument.Parse(json);
            value = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<T>(bson);
            return value;
        }

        public static string BsonToJson<T>(this T value, bool format = false, MongoDB.Bson.IO.JsonOutputMode jsonOutputMode = MongoDB.Bson.IO.JsonOutputMode.Strict)
            => MongoDB.Bson.BsonExtensionMethods.ToJson(value,
                new MongoDB.Bson.IO.JsonWriterSettings
                {
                    OutputMode = jsonOutputMode,
                    Indent = format
                }
            ).FromJson<JObject>()!.RemoveDuplicateKeys().ToJson()!;

        public static T FromBson<T>(this string bson)
        {
            var dict = bson.FromJson<Dictionary<string, object>>();
            if (dict?.ContainsKey("_t") == true && dict["_t"] is not string)
            {
                var lst = dict["_t"].ToJson()!.FromJson<List<string>>();
                dict["_t"] = lst.Last();
            }
            return MongoDB.Bson.BsonDocument.Parse(dict.ToJson()).BsonToObj<T>();
        }

        public static T BsonToObj<T>(this MongoDB.Bson.BsonDocument bsonDoc)
        {
            if (bsonDoc.Contains("_t") && bsonDoc["_t"].IsBsonArray)
            {
                bsonDoc["_t"] = bsonDoc["_t"].AsBsonArray.Last();
            }
            return MongoDB.Bson.Serialization.BsonSerializer.Deserialize<T>(bsonDoc);
        }

        public class AlwaysAllowDuplicateNamesBsonDocumentSerializer : MongoDB.Bson.Serialization.Serializers.BsonDocumentSerializer
        {
            protected override MongoDB.Bson.BsonDocument DeserializeValue(MongoDB.Bson.Serialization.BsonDeserializationContext context, MongoDB.Bson.Serialization.BsonDeserializationArgs args)
            {
                context = context.With(c => c.AllowDuplicateElementNames = true);
                return base.DeserializeValue(context, args);
            }

            public override MongoDB.Bson.BsonDocument Deserialize(MongoDB.Bson.Serialization.BsonDeserializationContext context, MongoDB.Bson.Serialization.BsonDeserializationArgs args)
            {
                context = context.With(c => c.AllowDuplicateElementNames = true);
                return base.Deserialize(context, args);
            }
        }
    }
}
