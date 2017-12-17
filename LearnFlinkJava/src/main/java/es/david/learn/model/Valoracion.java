package es.david.learn.model;

public class Valoracion {
    //userID: Integer, movieID: Integer ,rating: Integer, time: Long)
    private int userId;
    private String peliId;
    private int puntuacion;
    private long time;

    public Valoracion(int userId, String peliId, int puntuacion, long time) {
        this.userId = userId;
        this.peliId = peliId;
        this.puntuacion = puntuacion;
        this.time = time;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getPeliId() {
        return peliId;
    }

    public void setPeliId(String peliId) {
        this.peliId = peliId;
    }

    public int getPuntuacion() {
        return puntuacion;
    }

    public void setPuntuacion(int puntuacion) {
        this.puntuacion = puntuacion;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
