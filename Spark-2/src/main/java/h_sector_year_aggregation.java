import java.io.Serializable;

public class h_sector_year_aggregation implements Serializable {

    String sector_year;
    double volume_medio;
    double variazione_annuale;
    double quotazione_giornaliera_media;

    public h_sector_year_aggregation(){
    }

    public String getSector_year() {
        return sector_year;
    }

    public void setSector_year(String sector_year) {
        this.sector_year = sector_year;
    }

    public double getVolume_medio() {
        return volume_medio;
    }

    public void setVolume_medio(double volume_medio) {
        this.volume_medio = volume_medio;
    }

    public double getVariazione_annuale() {
        return variazione_annuale;
    }

    public void setVariazione_annuale(double variazione_annuale) {
        this.variazione_annuale = variazione_annuale;
    }

    public double getQuotazione_giornaliera_media() {
        return quotazione_giornaliera_media;
    }

    public void setQuotazione_giornaliera_media(double quotazione_giornaliera_media) {
        this.quotazione_giornaliera_media = quotazione_giornaliera_media;
    }

    @Override
    public String toString() {
        return "{" +
                "sector_year=" + sector_year +
                "volume_medio=" + volume_medio +
                ", variazione_annuale=" + variazione_annuale +
                ", quotazione_giornaliera_media=" + quotazione_giornaliera_media +
                '}';
    }
}
