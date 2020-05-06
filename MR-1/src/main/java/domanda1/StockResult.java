package domanda1;

public class StockResult {

    Double var_quotazione;
    Double prezzo_minimo;
    Double prezzo_massimo;
    Double volume_medio;

    public StockResult(Double var_quotazione, Double prezzo_minimo, Double prezzo_massimo, Double volume_medio) {
        this.var_quotazione = var_quotazione;
        this.prezzo_minimo = prezzo_minimo;
        this.prezzo_massimo = prezzo_massimo;
        this.volume_medio = volume_medio;
    }

    public StockResult() {
    }

    public Double getVar_quotazione() {
        return var_quotazione;
    }

    public void setVar_quotazione(Double var_quotazione) {
        this.var_quotazione = var_quotazione;
    }

    public Double getPrezzo_minimo() {
        return prezzo_minimo;
    }

    public void setPrezzo_minimo(Double prezzo_minimo) {
        this.prezzo_minimo = prezzo_minimo;
    }

    public Double getPrezzo_massimo() {
        return prezzo_massimo;
    }

    public void setPrezzo_massimo(Double prezzo_massimo) {
        this.prezzo_massimo = prezzo_massimo;
    }

    public Double getVolume_medio() {
        return volume_medio;
    }

    public void setVolume_medio(Double volume_medio) {
        this.volume_medio = volume_medio;
    }
}
