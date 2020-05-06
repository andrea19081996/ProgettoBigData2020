package domanda1;

public class TempOutputStatsResultWritable extends StatsResultWritable {

    @Override
    public String toString() {
        return this.var_quotazione + " " + this.prezzo_minimo + " " + this.prezzo_massimo + " " + this.volume_medio;
    }
}
