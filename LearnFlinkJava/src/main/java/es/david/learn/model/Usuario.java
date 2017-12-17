package es.david.learn.model;

public class Usuario {

    private int id;
    private String genero;
    private int edad;
    private String ocupacion;
    private String zip;

    public Usuario(int id, String genero, int edad, String ocupacion, String zip) {
        this.id = id;
        this.genero = genero;

        this.edad = edad;
        this.ocupacion = ocupacion;
        this.zip = zip;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getGenero() {
        return genero;
    }

    public void setGenero(String genero) {
        this.genero = genero;
    }

    public int getEdad() {
        return edad;
    }

    public void setEdad(int edad) {
        this.edad = edad;
    }

    public String getOcupacion() {
        return ocupacion;
    }

    public void setOcupacion(String ocupacion) {
        this.ocupacion = ocupacion;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

}
