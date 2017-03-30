package Storm.Transform;

import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Created by charlie on 21/03/17.
 */

public abstract class Transformer<T> {
    private static final Logger log = LoggerFactory.getLogger(Transformer.class);
    private static int DATE_LENGTH = 8;

    public abstract Values transform(T t) throws SQLException, ClassNotFoundException;

    /**
     * Transforms given date into correct format
     *
     * @param date
     * @return
     */
    String transformDate(String date) {
        System.out.println(date);

        if (date == null) return null;

        if (date.contains("."))
            return date.substring(0, date.lastIndexOf(".") + 4);
        else if (date.contains("Z"))
            return date.replace("Z", "").replace("T", " ");
        else
            return null;
    }

    /**
     * Transform integer day value into string value
     *
     * @param day String representation of Day
     * @return
     */
    String transformStatedDay(String day) {
        String statedDay;
        switch (day) {
            case "1":
                statedDay = "MON";
                break;
            case "2":
                statedDay = "TUE";
                break;
            case "3":
                statedDay = "WED";
                break;
            case "4":
                statedDay = "THU";
                break;
            case "5":
                statedDay = "FRI";
                break;
            case "6":
                statedDay = "SAT";
                break;
            case "7":
                statedDay = "SUN";
                break;
            default:
                statedDay = null;
        }
        return statedDay;
    }

    /**
     * Parse time into AM or PM.
     *
     * @param time Integer representation of time
     * @return AM/PM
     */
    String transformStatedTime(String time) {
        int iTime;
        try {
            iTime = Integer.parseInt(time);
        } catch (NumberFormatException nfe) {
            log.debug(String.format("Invalid StatedTime [%s], returning null", time));
            return null;
        }
        return (iTime >= 0 && iTime < 12) ? "AM" : "PM";
    }

}
