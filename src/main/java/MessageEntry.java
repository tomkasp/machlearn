import org.apache.spark.mllib.linalg.Vector;
import java.io.Serializable;
import java.util.HashSet;

/**
 * @author Agnieszka Pocha
 * Created by A046507 on 8/12/2015.
 */
public class MessageEntry implements Serializable{
    private Long mid = -1L; // message id
    private String body = "";
    private String bodySubjectNoStopWords = "";
    private String from = "";
    private Vector termFrequency ; // in our early days we called to it 'bag of words representation'
    private String subject = "";
    private String folder = "";
    private String date = "";
    private String owner = "";
    private java.util.Vector<String> to = new java.util.Vector<String>();
    private java.util.Vector<String> cc = new java.util.Vector<String>();
    private java.util.Vector<String> bcc = new java.util.Vector<String>();

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public Long getMid() {
        return mid;
    }

    public void setMid(Long mid) {
        this.mid = mid;
    }

    public String getBodySubjectNoStopWords() {
        return bodySubjectNoStopWords;
    }

    public void setBodySubjectNoStopWords(String bodyNoStopWords) {
        this.bodySubjectNoStopWords = bodyNoStopWords;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public Vector getTermFrequency() {
        return termFrequency;
    }

    public void setTermFrequency(Vector termFrequency) {
        this.termFrequency = termFrequency;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getFolder() {
        return folder;
    }

    public void setFolder(String folder) {
        this.folder = folder;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public java.util.Vector<String> getTo() {
        return to;
    }

    public void setTo(java.util.Vector<String> to) {
        this.to = to;
    }

    public java.util.Vector<String> getCc() {
        return cc;
    }

    public void setCc(java.util.Vector<String> cc) {
        this.cc = cc;
    }

    public java.util.Vector<String> getBcc() {
        return bcc;
    }

    public void setBcc(java.util.Vector<String> bcc) {
        this.bcc = bcc;
    }

    public java.util.Vector<String> getAllRecipientsUnique() {
        HashSet<String> uniqueRecipients = new HashSet<String>();
        if (!this.getTo().isEmpty()) uniqueRecipients.addAll(this.getTo());
        if (!this.getCc().isEmpty()) uniqueRecipients.addAll(this.getCc());
        if (!this.getBcc().isEmpty()) uniqueRecipients.addAll(this.getBcc());
        java.util.Vector<String> allRecipients = new java.util.Vector<String>(uniqueRecipients);
        return allRecipients;
    }

    public java.util.Vector<String> getAllRecipients(){
        java.util.Vector<String> allRecipients = new java.util.Vector<String>();
        if (!this.getTo().isEmpty()) allRecipients.addAll(this.getTo());
        if (!this.getCc().isEmpty()) allRecipients.addAll(this.getCc());
        if (!this.getBcc().isEmpty()) allRecipients.addAll(this.getBcc());
        return allRecipients;
    }

    @Override
    public boolean equals(Object mess_obj){
        if (!MessageEntry.class.isInstance(mess_obj)) return false;
        MessageEntry mess = (MessageEntry)mess_obj;
        // checking everything
        if(!this.mid.equals(mess.mid)) return false;
        if(!this.body.equals(mess.body)) return false;
        if(!this.bodySubjectNoStopWords.equals(mess.bodySubjectNoStopWords)) return false;
        if(!this.from.equals(mess.from)) return false;
        if(!this.subject.equals(mess.subject)) return false;
        if(!this.date.equals(mess.date)) return false;
        if(!this.owner.equals(mess.owner)) return false;
        if(!this.to.equals(mess.to)) return false;
        if(!this.cc.equals(mess.cc)) return false;
        if(!this.bcc.equals(mess.bcc)) return false;
        //comparing bag of words would be redundant, we already compared body, bag of words is derived from body
        // same goes to bodyNoStopWords
        return true;
    }

    @Override
    public int hashCode() {
        return (Long.toString(this.mid)
                +this.body+
                this.bodySubjectNoStopWords+
                this.from+
                this.subject+
                this.date
                +this.to.toString()+this.cc.hashCode()+this.bcc.hashCode() ).hashCode();
    }

    @Override
    public String toString() {
        return "ID: "+this.mid+"\nFROM: "+this.from+"\nDATE: "+this.date+"\nfolder: "+this.folder+"\nSUBJECT: "
                +this.subject+"\nBODY: "+this.body +"\nBODY no stop words: "+this.bodySubjectNoStopWords
                +"\nOWNER: "+this.owner+"\nTO: "+this.to.toString()+"\nCC: "+this.cc.toString()+"\nBCC: "+this.bcc.toString()+"\n\n\n";
    }

}
