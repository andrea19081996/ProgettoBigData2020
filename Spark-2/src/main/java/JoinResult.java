import java.io.Serializable;

public class JoinResult implements Serializable {
    String sector_year;
    double volume_medio;
    double variazione_annuale;
    double quotazione_giornaliera;
    int contatore_variazione_volume;
    double contatoreQuotazione;

    public JoinResult() {

    }

    @Override
    public String toString() {
        return "sector_year='" + sector_year + '\'' +
                ", volume_medio=" + volume_medio +
                ", variazione_annuale=" + variazione_annuale +
                ", quotazione_giornaliera_media=" + quotazione_giornaliera +
                ", contatore_variazione_volume=" + contatore_variazione_volume +
                ", contatoreQuotazione=" + contatoreQuotazione +
                '}';
    }


    public double getContatoreQuotazione() {
        return contatoreQuotazione;
    }

    public void setContatoreQuotazione(double contatoreQuotazione) {
        this.contatoreQuotazione = contatoreQuotazione;
    }

    public int getContatore_variazione_volume() {
        return contatore_variazione_volume;
    }

    public void setContatore_variazione_volume(int contatore_variazione_volume) {
        this.contatore_variazione_volume = contatore_variazione_volume;
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

    public double getQuotazione_giornaliera() {
        return quotazione_giornaliera;
    }

    public void setQuotazione_giornaliera(double quotazione_giornaliera) {
        this.quotazione_giornaliera = quotazione_giornaliera;
    }
}
