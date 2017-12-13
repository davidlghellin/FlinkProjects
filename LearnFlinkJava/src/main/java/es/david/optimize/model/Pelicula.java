package es.david.optimize.model;

import java.util.Set;

public class Pelicula {
    private String id;
    private String nombre;
    private Set<String> generos;



    public Pelicula(String id, String nombre, Set<String> generos) {
        this.id = id;
        this.nombre = nombre;
        this.generos = generos;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public Set<String> getGeneros() {
        return generos;
    }

    public void setGeneros(Set<String> generos) {
        this.generos = generos;
    }
}
