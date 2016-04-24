package nudt.ceaserz.hadoop_blazegraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

public class HypernymReducer extends Reducer<Text, Text, Text, Text> {

	private String sparqlEndPoint = "http://idc.answercow.org:3003/bigdata/namespace/lab681_KG/sparql";
	private SPARQLRepository repo = new SPARQLRepository(sparqlEndPoint);
	private String queryString = "SELECT ?s1 ?o2 ?s2 ?p3 WHERE {?s1 <http://www.w3.org/2000/01/rdf-schema#label> ?p1.?s2 ?o2 ?s1.?s2 <http://www.w3.org/2000/01/rdf-schema#label> ?p3}";
	// private String queryString = "SELECT ?s1 ?p1 WHERE {?s1
	// <http://www.w3.org/2000/01/rdf-schema#label> ?p1}";
	private RepositoryConnection con = null;
	private ValueFactory factory = null;
	private TupleQuery tupleQuery = null;

	public void setup(Context context) {
		repo.setUsernameAndPassword("rdfmanage", "bjgdrdfManage123");
		try {
			repo.initialize();
			con = repo.getConnection();
			tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			factory = repo.getValueFactory();
		} catch (RepositoryException e) {
			System.out.println("something wrong hadppend in initializing");
		} catch (MalformedQueryException e) {
			System.out.println("something wrong hadppend in prepareTupleQuery");
		}
	}

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		List<String> entities = new ArrayList();
		for (Text val : values) {
			entities.add(val.toString());
		}
		tupleQuery.setBinding("p1", factory.createLiteral(_key.toString(), "en"));
		try {
			TupleQueryResult result = tupleQuery.evaluate();
			while (result.hasNext()) {
				BindingSet bindingSet = result.next();
				String s1 = bindingSet.getValue("s1").toString();
				String o2 = bindingSet.getValue("o2").toString();
				String s2 = bindingSet.getValue("s2").toString();
				String p3 = bindingSet.getValue("p3").toString();
				String[] sp3 = p3.split("\"");
				if (entities.contains(sp3[1])) {
					String key = sp3[1] + "&&&&&" + _key.toString() + "&&&&&" + s2 + "&&&&&" + s1;
					context.write(new Text(key), new Text(new Text(o2)));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void cleanup(Context context) {
		try {
			con.close();
			repo.shutDown();
		} catch (RepositoryException e) {
			System.out.println("something wrong hadppend in RepositoryException");
		}
	}

}
