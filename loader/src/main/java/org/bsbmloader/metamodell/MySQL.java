package org.bsbmloader.metamodell;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.Query;
import org.bsbmloader.helpClass.OfferHelper;
import org.bsbmloader.helpClass.ProductHelper;
import org.bsbmloader.helpClass.ReviewHelper;



public class MySQL {
	private Connection connection ;
	private String jdbcUrl;
	private String username;
	private String password;	
	private  DataContext dc;
	
	public OfferHelper getOffer(){
		String[] value = new String[11];
		ArrayList<String[]> data = new ArrayList<String[]>();
		OfferHelper offer = new OfferHelper();
		buildConnection();
		try{
			Query query = createSimpleQuery("offer");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				value  = new String[11];	
				readDataFromRow(ds.getRow(), value);
				data.add(value);				
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		offer.setValue(data);
		offer.setProduct(getTitle("label","product"));
		offer.setVendor(getTitle("label","vendor"));
		offer.setProducer(getTitle("label","producer"));
		closeConnection();
		return offer;
		
	}
	
	public ReviewHelper getReview(){
		String[] value = new String[14];
		ArrayList<String[]> data = new ArrayList<String[]>();
		ReviewHelper review = new ReviewHelper();
		buildConnection();
		try{
			Query query = createSimpleQuery("review");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				value  = new String[15];
				
				readDataFromRow(ds.getRow(), value);
				data.add(value);				
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		review.setValue(data);
		review.setProduct(getTitle("label","product"));
		review.setPerson(getTitle("name","person"));
		review.setProducer(getTitle("label","producer"));
		closeConnection();
		return review;
		
	}

	public ArrayList<String[]> getAllVendor(){
		String[] value = new String[7];
		ArrayList<String[]> data = new ArrayList<String[]>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("vendor");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				value  = new String[7];
				readDataFromRow(ds.getRow(), value);
				data.add(value);
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		closeConnection();
		return data;	
	}
	
	public ArrayList<String[]> getAllPersons(){
		String[] value = new String[6];
		ArrayList<String[]> data = new ArrayList<String[]>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("person");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				value  = new String[6];
				readDataFromRow(ds.getRow(), value);
				data.add(value);
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		closeConnection();
		return data;	
	}
	
	public ArrayList<String[]> getAllProductFeature(){
		String[] value = new String[5];
		ArrayList<String[]> data = new ArrayList<String[]>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("productfeature");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				value  = new String[5];
				readDataFromRow(ds.getRow(), value);
				data.add(value);
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		closeConnection();
		return data;	
	}
	
	public ArrayList<String[]> getAllProductType(){
		String[] value = new String[6];
		ArrayList<String[]> data = new ArrayList<String[]>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("producttype");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				value  = new String[6];
				readDataFromRow(ds.getRow(), value);
				data.add(value);
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		closeConnection();
		return data;	
	}
	
	public ArrayList<ProductHelper> getAllProduct(){
	    ArrayList<ProductHelper> data = new ArrayList<ProductHelper>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("product");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
			    String[] value  = new String[18];
				readDataFromRow(ds.getRow(), value);
				ProductHelper product = new ProductHelper();
				product.setValue(value);
      			product.setProducer( getProducer(ds.getRow().getValue(3).toString()).get(0));
      			product.setProducefeature(getProductFeature(ds.getRow().getValue(0).toString()).getProducefeature());
      			product.setProducetype(getProductTyp(ds.getRow().getValue(0).toString()).getProducetype());
				data.add(product);
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		closeConnection();
		return data;
	}
	
	public void setConnectionProperties(String jdbcUrl, String username, String password){
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
	}
	
	private ArrayList<String[]> getProducer(String productNumber){
		String[] value = new String[7];
		ArrayList<String[]> data = new ArrayList<String[]>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("producer",Integer.parseInt(productNumber));
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				readDataFromRow(ds.getRow(), value);
				data.add(value);
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		closeConnection();
		return data;	
	}
	
	private ArrayList<String[]> getVendor(String productNumber){
		String[] value = new String[7];
		ArrayList<String[]> data = new ArrayList<String[]>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("vendor",Integer.parseInt(productNumber));
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				readDataFromRow(ds.getRow(), value);
				data.add(value);
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		closeConnection();
		return data;	
	}

	private Query createSimpleQuery(String tableName, int id){
			Query query = dc.query().from(tableName).selectAll().where("nr").eq(id).toQuery();
			return query;
	    }

    private Query createSimpleQuery(String tableName){
		Query query = dc.query().from(tableName).selectAll().toQuery();
		return query;
    }
	
	private void buildConnection(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(jdbcUrl,username,password);
			dc = DataContextFactory.createJdbcDataContext(connection);
		}catch (SQLException e) { 
			   e.printStackTrace(); 
        }catch(ClassNotFoundException e){
			 e.printStackTrace(); 
		}
	}
	
	private void closeConnection(){
		try{
			connection.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
		
		
	}
	
	private  ProductHelper getProductFeature(String productId){
		String[] value = new String[1];
		ArrayList<String[]> data = new ArrayList<String[]>();
		buildConnection();
		try{	
			Query query = dc.query().from("productfeature").innerJoin("productfeatureproduct").on("nr", "productFeature")
					      .select("productfeature.label").where("productfeatureproduct.product").eq(productId).toQuery();
            DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				value = new String[1];
				readDataFromRow(ds.getRow(), value);
				data.add(value);
			}
			ds.close(); 
		}catch(Exception e){
			e.printStackTrace();
		}
		ProductHelper tmp = new ProductHelper();
		tmp.setProducefeature(data);
		closeConnection();
		return tmp;
	}
	
	private ProductHelper getProductTyp(String productId){
		String[] value = new String[1];
		ArrayList<String[]> data = new ArrayList<String[]>();
		buildConnection();
		try{	
			value = new String[1];
			Query query = dc.query().from("producttype").innerJoin("producttypeproduct").on("nr", "producttype")
					      .select("producttype.label").where("producttypeproduct.product").eq(productId).toQuery();
            DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				readDataFromRow(ds.getRow(), value);
				data.add(value);
			}
			ds.close(); 
		}catch(Exception e){
			e.printStackTrace();
		}
		ProductHelper tmp = new ProductHelper();
		tmp.setProducetype(data);
		closeConnection();
		return tmp;
	}
	
	private String[] readDataFromRow(Row row, String[] value){
		for(int index = 0; index < row.size(); index++){
			if(row.getValue(index)!= null)
			   value[index] = row.getValue(index).toString();
			else
				value[index] = "null";
		}
		return value;
	}
	
	private ArrayList<String> getTitle(String title, String table ){
		ArrayList<String> data = new ArrayList<String>();
		buildConnection();
		try {
			Query query = dc.query().from(table).select(title).toQuery();
			dc.executeQuery(query);
			DataSet ds = dc.executeQuery(query);
				while(ds.next()){
					data.add(ds.getRow().getValue(0).toString());
				}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		closeConnection();
		return data;
		
	}

}
