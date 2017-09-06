import lombok.Data;

import java.util.Date;

@Data
public class Master {
    private int borough;
    private String crfn;
    private double transaction_amount;
    private Date doc_date;
    private String doc_type;
    private String document_id;
    private Date good_through_date;
    private Date modified_date;
    private double percent_transfer;
    private String record_type;
    private Date recorded_date;
    private int reel_number;
    private int reel_page;
    private int reel_year;
    private String description;
}
