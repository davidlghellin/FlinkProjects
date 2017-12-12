package es.david.movie;

import java.util.HashSet;
import java.util.Set;

public class Pelicula {
    private String nombre;
    private Set<String> generos;

    public Pelicula(String nombre, Set<String> generos) {
        this.nombre = nombre;
        this.generos = generos;
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
