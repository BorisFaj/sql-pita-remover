# rosqltta

## History

At some moment came the necesity of translate a bunch (200 aprox) pure HIVE queries to SparkSQL.
Both sintaxis were pretty similar but not exactly (at least in that versions).

Not happy enough, I were required to change many (almost all) table and field names to fit into new normaliced name politics.

When you have complex sentencies, with subqueries, alias, and so on, it becomes a hard (and pain in the ass) work. You 
can focus, listen destructive music and code everything, but then you have to test it and ensure it is correct.
So I didn't want to translate it manually.

That is how this library came to life. I figured out the HIVE grammar using reverse engineering and it own documentation.
The library is able to construct the Abstract Sintax Tree (AST) and make the changes at exact points of the sentence where 
they are lowest level, allowing to propagate the changes until the highest level. 

i.e: using an alias in a subquery nested in other subquery with a different alias the name should not change at this point. 
But the parent query must know the children keep same name and at the same time it (parent) has to rename the field at right 
part of the sentence, not when asking to the subquery but when it is setting the name at higher SELECT statment.

The project got stucked because the data never came into the system, so this library was never used. And actually my project 
managers never asked me to code it, because they though I was going to do everything manually.

The data came one year later and I was already to assigned to a different project. So here is the library just in case anyone get 
inspired by it, and so I dont lose it =D

# ToDo
* real examples
* more documentation
