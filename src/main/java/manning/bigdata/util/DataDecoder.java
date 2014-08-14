package manning.bigdata.util;

import manning.bigdata.swa.*;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: ivaramme
 * Date: 7/15/14
 */
public class DataDecoder {
    public static Data decodeJsonMessage(JSONObject jsonObject, String messageType) {
        Data data = null;

        if (messageType.equals("page")) {
            data = decodePage(jsonObject);
        } /* else if (messageType.equals("person")) {
            data = decodePerson(jsonObject);
        }*/ else if (messageType.equals("equiv")) {
            data = decodeEquiv(jsonObject);
        } else if (messageType.equals("pageview")) {
            data = decodePageView(jsonObject);
        }
        return data;
    }

    private static Data decodePage(JSONObject jsonObject) {
        String pedigree = (String) jsonObject.get("pedigree");
        String url = (String) jsonObject.get("url");
        return null;
    }

    public static List<Data> decodePerson(JSONObject jsonObject) {
        List<Data> res = new ArrayList<Data>();

        System.out.println("Decoding Person: " + jsonObject);
        String pedigree = (String) jsonObject.get("pedigree");
        String personId = (String) jsonObject.get("personid");
        String gender = (String) jsonObject.get("gender");

        GenderType genderType = null;
        if (gender.equals("MALE")) {
            genderType = GenderType.MALE;
        } else {
            genderType = GenderType.FEMALE;
        }

        String fullname = (String) jsonObject.get("fullname");
        String city = (String) jsonObject.get("city");
        String state = (String) jsonObject.get("state");
        String country = (String) jsonObject.get("country");

        PersonID personID = new PersonID();
        if (personId.startsWith("cookie")) {
            personID.setCookie(personId);
        } else {
            personID.setUser_id(Long.parseLong(personId));
        }

        DataUnit dataUnit = new DataUnit();
        PersonProperty personProperty = new PersonProperty();
        PersonPropertyValue personPropertyValue = new PersonPropertyValue();

        Location location = new Location();
        location.setCity(city);
        location.setState(state);
        location.setCountry(country);

        personPropertyValue.setLocation(location);
        personProperty.setProperty(personPropertyValue);
        personProperty.setId(personID);
        dataUnit.setPerson_property(personProperty);
        res.add(getData(pedigree, dataUnit));

        personProperty = new PersonProperty();
        personPropertyValue = new PersonPropertyValue();
        personPropertyValue.setGender(genderType);
        personProperty.setProperty(personPropertyValue);
        personProperty.setId(personID);
        dataUnit = new DataUnit();
        dataUnit.setPerson_property(personProperty);
        res.add(getData(pedigree, dataUnit));

        personProperty = new PersonProperty();
        personPropertyValue = new PersonPropertyValue();
        personPropertyValue.setFull_name(fullname);
        personProperty.setProperty(personPropertyValue);
        personProperty.setId(personID);
        dataUnit = new DataUnit();
        dataUnit.setPerson_property(personProperty);
        res.add(getData(pedigree, dataUnit));

        return res;
    }

    private static Data getData(String timestamp, DataUnit dataUnit) {
        Pedigree pedigree = new Pedigree();
        pedigree.setTrue_as_of_secs(Integer.parseInt(timestamp));

        Data data = new Data();
        data.setPedigree(pedigree);
        data.setDataunit(dataUnit);

        return data;
    }

    private static Data decodeEquiv(JSONObject jsonObject) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    private static Data decodePageView(JSONObject jsonObject) {
        return null;
    }
}
