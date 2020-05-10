import org.apache.hadoop.io.WritableComparable;

import java.util.Date;

public class h_elementary  {
    String sector;
    double prezzo_chisura;
    double volume;
    Date data_min;
    Date data_max;
    double prezzo_chiusura_min;
    double prezzo_chiusura_max;
    double contatore;

    @Override
    public String toString() {
        return "h_prova{" +
                "sector='" + sector + '\'' +
                ", prezzo_chisura=" + prezzo_chisura +
                ", volume=" + volume +
                ", data_min=" + data_min +
                ", data_max=" + data_max +
                ", prezzo_chiusura_min=" + prezzo_chiusura_min +
                ", prezzo_chiusura_max=" + prezzo_chiusura_max +
                ", contatore=" + contatore +
                '}';
    }

    public Date getData_min() {
        return data_min;
    }

    public void setData_min(Date data_min) {
        this.data_min = data_min;
    }

    public Date getData_max() {
        return data_max;
    }

    public void setData_max(Date data_max) {
        this.data_max = data_max;
    }


    public double getPrezzo_chiusura_min() {
        return prezzo_chiusura_min;
    }

    public void setPrezzo_chiusura_min(double prezzo_chiusura_min) {
        this.prezzo_chiusura_min = prezzo_chiusura_min;
    }

    public double getPrezzo_chiusura_max() {
        return prezzo_chiusura_max;
    }

    public void setPrezzo_chiusura_max(double prezzo_chiusura_max) {
        this.prezzo_chiusura_max = prezzo_chiusura_max;
    }

    public double getContatore() {
        return contatore;
    }

    public void setContatore(double contatore) {
        this.contatore = contatore;
    }

    public h_elementary(){
    }


    public String getSector() {
        return sector;
    }

    public void setSector(String sector) {
        this.sector = sector;
    }

    public double getPrezzo_chisura() {
        return prezzo_chisura;
    }

    public void setPrezzo_chisura(double prezzo_chisura) {
        this.prezzo_chisura = prezzo_chisura;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }


    public void addVolume(double parseDouble) {
        this.volume+=parseDouble;
    }

    public void addPrezzoChiusura(double parseDouble) {
        this.prezzo_chisura+=parseDouble;
    }

    public void addContatore() {
        this.contatore++;
    }

}