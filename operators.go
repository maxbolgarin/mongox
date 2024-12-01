package mongox

// Comparison Operators
// https://www.mongodb.com/docs/manual/reference/operator/query-comparison/
const (
	// $eq matches values that are equal to a specified value.
	Eq = "$eq"

	// $gt matches values that are greater than a specified value.
	Gt = "$gt"

	// $gte matches values that are greater than or equal to a specified value.
	Gte = "$gte"

	// $in matches any of the values specified in an array.
	In = "$in"

	// $lt matches values that are less than a specified value.
	Lt = "$lt"

	// $lte matches values that are less than or equal to a specified value.
	Lte = "$lte"

	// $ne matches all values that are not equal to a specified value.
	Ne = "$ne"

	// $nin matches none of the values specified in an array.
	Nin = "$nin"
)

// Logical Operators
// https://www.mongodb.com/docs/manual/reference/operator/query-logical/
const (
	// $and joins query clauses with a logical AND, returning all documents that match the conditions of both clauses.
	And = "$and"

	// $not inverts the effect of a query predicate, returning documents that do not match the query predicate.
	Not = "$not"

	// $nor joins query clauses with a logical NOR, returning all documents that fail to match both clauses.
	Nor = "$nor"

	// $or joins query clauses with a logical OR, returning all documents that match the conditions of either clause.
	Or = "$or"
)

// Element Operators
// https://www.mongodb.com/docs/manual/reference/operator/query-element/
const (
	// $exists matches documents that have the specified field.
	Exists = "$exists"

	// $type selects documents if a field is of the specified type.
	Type = "$type"
)

// Evaluation Operators
// https://www.mongodb.com/docs/manual/reference/operator/query-evaluation/
const (
	// $expr allows use of aggregation expressions within the query language.
	Expr = "$expr"

	// $jsonSchema validates documents against the given JSON Schema.
	JsonSchema = "$jsonSchema"

	// $mod performs a modulo operation on the value of a field and selects documents with a specified result.
	Mod = "$mod"

	// $regex selects documents where values match a specified regular expression.
	Regex = "$regex"

	// $text performs text search.
	Text = "$text"

	// $where matches documents that satisfy a JavaScript expression.
	Where = "$where"
)

// Geospatial Operators
// https://www.mongodb.com/docs/manual/reference/operator/query-geospatial/
const (
	// $geoIntersects selects geometries that intersect with a GeoJSON geometry.
	GeoIntersects = "$geoIntersects"

	// $geoWithin selects geometries within a bounding GeoJSON geometry.
	GeoWithin = "$geoWithin"

	// $near returns geospatial objects in proximity to a point.
	Near = "$near"

	// $nearSphere returns geospatial objects in proximity to a point on a sphere.
	NearSphere = "$nearSphere"
)

// Array Operators
const (
	// $all matches arrays that contain all elements specified in the query.
	All = "$all"

	// $elemMatch selects documents if element in the array field matches all the specified $elemMatch conditions.
	ElemMatch = "$elemMatch"

	// $size selects documents if the array field is a specified size.
	Size = "$size"
)

// Bitwise Operators
// https://www.mongodb.com/docs/manual/reference/operator/query-bitwise/
const (
	// $bitsAllClear matches numeric or binary values in which a set of bit positions all have a value of 0.
	BitsAllClear = "$bitsAllClear"

	// $bitsAllSet matches numeric or binary values in which a set of bit positions all have a value of 1.
	BitsAllSet = "$bitsAllSet"

	// $bitsAnyClear matches numeric or binary values in which any bit from a set of bit positions has a value of 0.
	BitsAnyClear = "$bitsAnyClear"

	// $bitsAnySet matches numeric or binary values in which any bit from a set of bit positions has a value of 1.
	BitsAnySet = "$bitsAnySet"
)

// Projection Operators
// https://www.mongodb.com/docs/manual/reference/operator/query-projection/
const (
	// $ projects the first element in an array that matches the query condition.
	ProjectionElem = "$"

	// $elemMatch projects the first element in an array that matches the specified $elemMatch condition.
	ProjectionElemMatch = "$elemMatch"

	// $meta projects the document's score assigned during the $text operation.
	ProjectionMeta = "$meta"

	// $slice limits the number of elements projected from an array. Supports skip and limit slices.
	ProjectionSlice = "$slice"
)

// Miscellaneous Operators
// https://www.mongodb.com/docs/manual/reference/operator/query-miscellaneous/
const (
	// $rand generates a random float between 0 and 1.
	Rand = "$rand"

	// $natural is a special hint that can be provided via the sort() or hint() methods to force either a forward or reverse collection scan.
	Natural = "$natural"
)

// Field Update Operators
// https://www.mongodb.com/docs/manual/reference/operator/update-field/
const (
	// CurrentDate sets the value of a field to current date, either as a Date or a Timestamp.
	CurrentDate = "$currentDate"

	// Inc increments the value of the field by the specified amount.
	Inc = "$inc"

	// Min only updates the field if the specified value is less than the existing field value.
	Min = "$min"

	// Max only updates the field if the specified value is greater than the existing field value.
	Max = "$max"

	// Mul multiplies the value of the field by the specified amount.
	Mul = "$mul"

	// Rename renames a field.
	Rename = "$rename"

	// Set sets the value of a field in a document.
	Set = "$set"

	// SetOnInsert sets the value of a field if an update results in an insert of a document.
	SetOnInsert = "$setOnInsert"

	// Unset removes the specified field from a document.
	Unset = "$unset"
)

// Array Update Operators
// https://www.mongodb.com/docs/manual/reference/operator/update-array/
const (
	// ArrayElem acts as a placeholder to update the first element that matches the query condition.
	ArrayElem = "$"

	// AllArrayElems acts as a placeholder to update all elements in an array for the documents that match the query condition.
	AllArrayElems = "$[]"

	// IdentifiedArrayElem acts as a placeholder to update all elements that match the arrayFilters condition.
	IdentifiedArrayElem = "$[<identifier>]"

	// AddToSet adds elements to an array only if they do not already exist in the set.
	AddToSet = "$addToSet"

	// Pop removes the first or last item of an array.
	Pop = "$pop"

	// Pull removes all array elements that match a specified query.
	Pull = "$pull"

	// Push adds an item to an array.
	Push = "$push"

	// PullAll removes all matching values from an array.
	PullAll = "$pullAll"
)

// Array Modifiers
const (
	// Each modifies the $push and $addToSet operators to append multiple items for array updates.
	Each = "$each"

	// Position modifies the $push operator to specify the position in the array to add elements.
	Position = "$position"

	// Slice modifies the $push operator to limit the size of updated arrays.
	Slice = "$slice"

	// Sort modifies the $push operator to reorder documents stored in an array.
	Sort = "$sort"
)

// Bitwise Update Operator
// https://www.mongodb.com/docs/manual/reference/operator/update-bitwise/
const (
	// Bit performs bitwise AND, OR, and XOR updates of integer values.
	Bit = "$bit"
)
