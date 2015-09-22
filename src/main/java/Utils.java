import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;

import java.util.*;

/**
 * @author Agnieszka Pocha
 *         Created by A046507 on 9/11/2015.
 */
public class Utils {

    //Cleaning: changing all undisclosed to: undisclosed-recipients@enron.com
    public static Vector<String> splitAndUndiscloseAddresses(String s) {
        if (s == null || s.isEmpty()) return new Vector<String>();
        // removing white characters, then splitting
        String[] addresses = s.replaceAll("\\s", "").split(",");
        for (int i = 0; i < addresses.length; i++) {
            if (addresses[i].contains("undisclosed")) addresses[i] = "undisclosed-recipients@enron.com";
        }

        return new Vector<String>(Arrays.asList(addresses));
    }

    public static Boolean isStringEmpty(String s) {
        if (s == null) return true;
        // if only white characters
        return s.replaceAll("\\s", "").equals("");
    }

    public static String removeStopWords(String text) {
        String[] stop_words = {"about", "all", "am", "an", "and", "are", "as", "at", "be", "been", "but", "by", "can",
                "cannot", "did", "do", "does", "doing", "done", "for", "from", "had", "has", "have", "having", "if",
                "in", "is", "it", "its", "of", "on", "that", "the", "they", "these", "this", "those", "to", "too",
                "want", "wants", "was", "what", "which", "will", "with", "would"};

        // text should already be lowercased but better safe than sorry
        // removing redundant white characters
        LinkedList<String> temp = new LinkedList<String>(Arrays.asList(cleanText(text).split(" ")));
        // Removes all of this collection's elements that are also contained in the specified collection (optional operation). (Javadoc)
        temp.removeAll(Arrays.asList(stop_words));
        // changing array of strings into a string, delimiter is ' '
        return StringUtils.join(temp, ' ');
    }

    public static String cleanText(String text) {
        return text.toLowerCase().replaceAll("\\s+", " ").trim();
    }

    public static MessageEntry unify(MessageEntry messageEntry){
        Map<String, String> unification_table = Utils.getUnificationMap();

        if (unification_table.containsKey(messageEntry.getFrom()))
            messageEntry.setFrom(unification_table.get(messageEntry.getFrom()));


        Vector<String> to = messageEntry.getTo();
        for (int i = 0; i < to.size(); ++i) {
            if(unification_table.containsKey(to.get(i)))
                to.set(i, unification_table.get(to.get(i)));
        }
        messageEntry.setTo(to);

        Vector<String> cc = messageEntry.getCc();
        for (int i = 0; i < cc.size(); ++i) {
            if(unification_table.containsKey(cc.get(i)))
                cc.set(i, unification_table.get(cc.get(i)));
        }
        messageEntry.setCc(cc);

        Vector<String> bcc = messageEntry.getTo();
        for (int i = 0; i < bcc.size(); ++i) {
            if(unification_table.containsKey(bcc.get(i)))
                bcc.set(i, unification_table.get(bcc.get(i)));
        }
        messageEntry.setBcc(bcc);

        return messageEntry;
    }

    public static Map<String, String> getUnificationMap() {
        final Map<String, String> unificationMap = new TreeMap<String, String>();
        unificationMap.put("'vkaminski@aol.com'", "vince.kaminski@enron.com");
        unificationMap.put("vkamins@enron.com", "vince.kaminski@enron.com");
        unificationMap.put("j.kaminski@enron.com", "vince.kaminski@enron.com");
        unificationMap.put("kaminski@enron.com", "vince.kaminski@enron.com");
        unificationMap.put("vince.j.kaminski@enron.com", "vince.kaminski@enron.com");
        unificationMap.put("k..allen@enron.com", "phillip.allen@enron.com");
        unificationMap.put("f..brawner@enron.com", "sandra.brawner@enron.com");
        unificationMap.put("f..campbell@enron.com", "larry.campbell@enron.com");
        unificationMap.put("w..delainey@enron.com", "david.delainey@enron.com");
        unificationMap.put("j..farmer@enron.com", "daren.farmer@enron.com");
        unificationMap.put("m..forney@enron.com", "john.forney@enron.com");
        unificationMap.put("l..gay@enron.com", "rob.gay@enron.com");
        unificationMap.put("randall.gay@enron.com", "rob.gay@enron.com");
        unificationMap.put("c..giron@enron.com", "darron.giron@enron.com");
        unificationMap.put("e..haedicke@enron.com", "mark.haedicke@enron.com");
        unificationMap.put("judy.hernandez@enron.com", "juan.hernandez@enron.com");
        unificationMap.put("t..hodge@enron.com", "jeffrey.hodge@enron.com");
        unificationMap.put("john.hodge@enron.com", "jeffrey.hodge@enron.com");
        unificationMap.put("horton@enron.com", "stanley.horton@enron.com");
        unificationMap.put("stan.horton@enron.com", "stanley.horton@enron.com");
        unificationMap.put("steve.kean@enron.com", "steven.kean@enron.com");
        unificationMap.put("j..kean@enron.com", "steven.kean@enron.com");
        unificationMap.put("f..keavey@enron.com", "peter.keavey@enron.com");
        unificationMap.put("lavorato@enron.com", "john.lavorato@enron.com");
        unificationMap.put("rosalee.fleming@enron.com", "kenneth.lay@enron.com");
        unificationMap.put("h..lewis@enron.com", "andrew.lewis@enron.com");
        unificationMap.put("m..love@enron.com", "phillip.love@enron.com");
        unificationMap.put("a..martin@enron.com", "thomas.martin@enron.com");
        unificationMap.put("mike.mcconnell@ect.enron.com", "mark.mcconnell@enron.com");
        unificationMap.put("mike.mcconnell@enron.com", "mark.mcconnell@enron.com");
        unificationMap.put("l..mims@enron.com", "patrice.mims@enron.com");
        unificationMap.put("w..pereira@enron.com", "susan.pereira@enron.com");
        unificationMap.put("m..presto@enron.com", "kevin.presto@enron.com");
        unificationMap.put("b..sanders@enron.com", "richard.sanders@enron.com");
        unificationMap.put("m..scott@enron.com", "susan.scott@enron.com");
        unificationMap.put("a..shankman@enron.com", "jeffrey.shankman@enron.com");
        unificationMap.put("jeff.shankman@enron.com", "jeffrey.shankman@enron.com");
        unificationMap.put("s..shively@enron.com", "hunter.shively@enron.com");
        unificationMap.put("jeff.skilling@enron.com", "jeffrey.skilling@enron.com");
        unificationMap.put("sherri.sera@enron.com", "jeffrey.skilling@enron.com");
        unificationMap.put("fletch.sturm@enron.com", "fletcher.sturm@enron.com");
        unificationMap.put("j..sturm@enron.com", "fletcher.sturm@enron.com");
        unificationMap.put("legal <.taylor@enron.com>", "mark.taylor@enron.com");
        unificationMap.put("e.taylor@enron.com", "mark.taylor@enron.com");
        unificationMap.put("m..tholt@enron.com", "jane.tholt@enron.com");
        unificationMap.put("d..thomas@enron.com", "paul.thomas@enron.com");
        unificationMap.put("houston <.ward@enron.com>", "kim.ward@enron.com");
        unificationMap.put("s..ward@enron.com", "kim.ward@enron.com");
        unificationMap.put("v.weldon@enron.com", "charles.weldon@enron.com");
        unificationMap.put("trading <.williams@enron.com>", "jason.williams@enron.com");
        unificationMap.put("benjami.rogers@enron.com", "benjamin.rogers@enron.com");
        unificationMap.put("benjamin.rogers@ect.enron.com", "benjamin.rogers@enron.com");
        unificationMap.put("eric.saibi@houston.rr.com", "eric.saibi@enron.com");
        unificationMap.put("dutch.quigley@ect.enron.com", "dutch.quigley@enron.com");
        unificationMap.put("jim.schwieger@rr.houston.com", "jim.schwieger@enron.com");
        unificationMap.put("sara.shackleton@db.com", "sara.shackleton@enron.com");
        unificationMap.put("c.germany@enron.com", "chris.germany@enron.com");
        unificationMap.put("kay.mann@worldnet.att.net", "kay.mann@enron.com");
        unificationMap.put("kay.mann@att.worldnet.net", "kay.mann@enron.com");
        unificationMap.put("craig.dean@enron.com", "clint.dean@enron.com");
        unificationMap.put("dutch.quigley@ect.enron.com", "dutch.quigley@enron.com");
        unificationMap.put("scott.neal@worldnet.att.net", "scott.neal@enron.com");
        unificationMap.put("chris.dorland@msn.com", "chris.dorland@enron.com");
        unificationMap.put("w..white@enron.com", "stacey.white@enron.com");
        unificationMap.put("stacey.white@ect.enron.com", "stacey.white@enron.com");
        unificationMap.put("kevin.hyatt@carsmart.com", "kevin.hyatt@enron.com");
        unificationMap.put("sally.beck@alumni.utexas.net", "sally.beck@enron.com");
        unificationMap.put("d..steffes@enron.com", "james.steffes@enron.com");
        unificationMap.put("rod.hayslett@enron.c", "rod.hayslett@enron.com");
        unificationMap.put("l..mims@enron.com", "patrice.mims@enron.com");
        unificationMap.put("t..lucci@enron.com", "paul.lucci@enron.com");
        unificationMap.put("geof.storey@enron.com", "geoff.storey@enron.com");
        unificationMap.put("jonathon.mckay@enron.com", "jonathan.mckay@enron.com");
        unificationMap.put("jdasovic@enron.com", "jeff.dasovich@enron.com");
        unificationMap.put("michele.lokay@enron.com", "michelle.lokay@enron.com");
        return unificationMap;
    }

    public static MessageEntry rowToMessageEntry(Row row){
        MessageEntry enronEntry = new MessageEntry();
        enronEntry.setBcc(Utils.splitAndUndiscloseAddresses(row.get(0).toString()));
        enronEntry.setBody(row.get(1).toString());
        enronEntry.setCc(Utils.splitAndUndiscloseAddresses(row.get(2).toString()));
        enronEntry.setDate(row.get(3).toString());
        enronEntry.setFolder(row.get(4).toString());
        enronEntry.setFrom(row.get(5).toString());
        enronEntry.setMid(row.getLong(6));
        enronEntry.setOwner(row.get(7).toString());
        enronEntry.setSubject(row.get(8).toString());
        enronEntry.setTo(Utils.splitAndUndiscloseAddresses(row.get(9).toString()));
        return enronEntry;
    }


}
