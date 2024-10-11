Acá va el código que representa las entidades usando Algebra Relacional, antes de pasarlo a un gráfico bonito

Estilos: 
- el nombre de las tablas comienza con mayúsculas, seguidas de un guión bajo para separar palabras.
- el nombre de las propiedades se escribe con las palabras todas juntas (sin '_'), y con la primer letra de cada palabra mayúscula.

Entidades 
```
Jugadores(Id, Nombre, EquipoId)
Equipos(Id, Nombre, LigaId)
Ligas(Id, Nombre, PaisId)
Paises(Id, Nombre)
Partidos(Id, LigaId, Resultado, IdLocal, IdVicitante, GolesLocal, temporada)
```

Relaciones (es redundante, las entidades son relaciones, pero es para colocar tablas que sólo representan una relación entre dos entidades, particularmente "una a muchas")

```
Jugador_De_Equipo(id_jugador, id_equipo, rol, temporada)
```
En este caso el rol es el número de la camiseta

Observación: Quitamos Países y Ligas pues trabajermos sobre España.

```
Jugadores(Id, Nombre, EquipoId)
Equipos(Id, Nombre, LigaId)
Ligas(Id, Nombre, PaisId)
Paises(Id, Nombre)
Partidos(Id, LigaId, Resultado, IdLocal, IdVicitante, GolesLocal, temporada)
```
