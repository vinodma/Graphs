//Generates connected paths of size n from seeded vertices

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx._

def makeMap(x: (VertexId,String)*) = Map(x: _*)
type SPMap = Map[VertexId, String]
//val sourceId=Array(8L,15L)
val sourceId=sc.broadcast(common.map(IPv4ToLong _).collect)
val graph: Graph[Int, Int] =
        GraphGenerators.rmatGraph(sc,requestedNumVertices=10, numEdges = 5)
val initialGraph = graph.mapVertices{(id, _) =>
if (sourceId.value.contains(id)) makeMap(id->id.toString) else makeMap() }.mapEdges(e=>e.attr.toLong)
def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {
if(spmap1.isEmpty && spmap2.isEmpty){
return makeMap();
}
if(spmap1.isEmpty){
return spmap2;
}
if(spmap2.isEmpty){
return spmap1;
}
spmap1++spmap2;
}
  
  
def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d.concat(",".concat(v.toString)) )}
val initialGraph = graph.mapVertices{(id, _) =>
     if (id == sourceId) makeMap(id->id.toString) else makeMap()
     }	 
def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {

addMaps(attr, msg)

}
def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
if(!edge.srcAttr.isEmpty){
var lst=Iterator[(VertexId, SPMap)]()
val mp=edge.srcAttr.asInstanceOf[SPMap]
for ((k,v)<-mp){
if(v.split(",")(v.split(",").length-1)!=edge.dstId.toString){
val dstmap=makeMap(k->v.concat(",".concat(edge.dstId.toString)))
lst = lst++Iterator((edge.dstId, dstmap))
}
}
lst
}
else
{
Iterator.empty
}
//val newAttr = incrementMap(edge.dstAttr)
//if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
//else Iterator.empty
}	
val initialMessage = makeMap()
Pregel(initialGraph, initialMessage,maxIterations=10)(vertexProgram _, sendMessage _, addMaps _)
