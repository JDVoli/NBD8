import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class Main {
    private static final String BUCKET_NAME = "s15843";
    private static final String BUCKET_TYPE = "default";
    private static final String KEY = "zad8";

    private static String jsonInput = "{\"name\":\"Marlon Brando\",\"age\":80,\"isStudent\":false,\"nationality\":\"USA\"}";
    private static String updatedJson = "{\"name\":\"Marlon Brando\",\"age\":80,\"isStudent\":false,\"nationality\":\"USA\",\"isDead\":true}";

    public static void main(String[] args) {

        RiakClient client = null;
        RiakObject res = null;
        try {
            client = RiakClient.newClient("127.0.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        System.out.println("Dodawany obiekt do bazy: "+ jsonInput);

        insertItem(client, KEY);
        res = getItem(client, KEY);
        printResult("\nPobrany obiekt po dodaniu: ",res);


        updateItem(client, res, KEY);
        res = getItem(client,KEY);
        printResult("Pobrany obiekt po zaktualizowaniu: ", res);


        deleteItem(client,KEY);
        res = getItem(client,KEY);
        printResult("Pobrany obiekt po usunieciu: ", res);


        client.shutdown();
    }


    public static void insertItem(RiakClient client, String key){
        try {
            Namespace ns = new Namespace(BUCKET_TYPE, BUCKET_NAME);
            Location location = new Location(ns, key);
            RiakObject riakObject = new RiakObject().setContentType("application/json");
            riakObject.setValue(BinaryValue.create(jsonInput));
            StoreValue store = new StoreValue.Builder(riakObject)
                    .withLocation(location).build();

            client.execute(store);

        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static RiakObject getItem(RiakClient client, String key){
        try {
            Namespace ns = new Namespace(BUCKET_TYPE,BUCKET_NAME);
            Location loc = new Location(ns,key);
            FetchValue fv = new FetchValue.Builder(loc).build();

            FetchValue.Response response = client.execute(fv);
            RiakObject obj = response.getValue(RiakObject.class);

            return obj;
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void updateItem(RiakClient client, RiakObject obj, String key){
        try {
            obj.setValue(BinaryValue.create(updatedJson));

            Location updateLoc = new Location(new Namespace(BUCKET_TYPE, BUCKET_NAME),key);
            StoreValue store = new StoreValue.Builder(obj)
                    .withLocation(updateLoc).build();

            client.execute(store);

        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void deleteItem(RiakClient client, String key){
        Location deleteLoc = new Location(new Namespace(BUCKET_TYPE, BUCKET_NAME),key);
        DeleteValue delete = new DeleteValue.Builder(deleteLoc).build();
        try {
            client.execute(delete);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void printResult(String msg, RiakObject obj){
        if(obj != null) {
            System.out.println(msg + obj.getValue());
        }else {
            System.out.println(msg + obj);
        }

    }
}
