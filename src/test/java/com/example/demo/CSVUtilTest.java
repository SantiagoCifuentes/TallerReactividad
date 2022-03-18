package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {

    @Test
    void converterData() {
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void stream_filtrarJugadoresMayoresA35() {
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }


    @Test
    void reactive_filtrarJugadoresMayoresA35() {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })

                .buffer(100)
                .flatMap(playerA -> listFlux
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a -> a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
        System.out.println(listFilter.block().size());
    }

    //jugadores mayores a 34
    @Test
    void filtrarJugadoresMayoresA34() {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 34 && player.club.equals("Juventus"))
                .distinct()
                .collectMultimap(Player::getClub);


        listFilter.block().forEach((equipo, players) ->
        {
            System.out.println(equipo);
            players.forEach(player ->
            {
                System.out.println("Nombre: " + player.name + ", Edad: " + player.age);
                assert player.club.equals("Juventus");
                assert player.age >= 34;
            });

        });
    }

    //filtrado por nacionalidad
    @Test
    void filtrarPorNacionalidad() {

        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux


                .sort(Comparator.comparing(Player::getWinners).reversed())
                .filter(player -> player.winners > 84)
                .distinct()

                .collectMultimap(Player::getNational);


        //imprime la lista de países con sus respectivos
        // jugadores con sus victorias de mayor a menor, teniendo en cuenta el filter aplicado en la línea 102
        //si se borra este filter aparecerían todos los paises con sus jugadores
        listFilter.block().forEach((nacionalidad, players) ->
        {
            System.out.println(nacionalidad);
            players.forEach(player ->
            {
                System.out.println("Nombre: " + player.name + ", Nacionalidad: " + player.national + ", Victorias: " + player.winners);

            });
        });


    }


}
