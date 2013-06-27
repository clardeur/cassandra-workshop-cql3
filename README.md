Workshop Cassandra
=================

## Installer Cassandra

Télécharger la version 1.2 de Cassandra à l'adresse suivante: 
- http://www.apache.org/dyn/closer.cgi?path=/cassandra/1.2.6/apache-cassandra-1.2.6-bin.tar.gz

Ajoutez Cassandra dans le PATH.

### Sous Linux/OSX

    $ export CASSANDRA_HOME=<...racine du répertoire Cassandra...>
    $ export PATH=$PATH:$CASSANDRA_HOME/bin

### Sous Windows

    set CASSANDRA_HOME=<...racine du répertoire Cassandra...>
    set PATH=%PATH%;%CASSANDRA_HOME\bin

## Réglages complémentaires

Pour activer le protocole CQL natif remplacez dans `$CASSANDRA_HOME/cassandra.yaml` :

    start_native_transport: true

## Création du Keyspace (Database)

Exécuter le script `keyspace.txt` :

    $ cqlsh < ./src/test/resources/scripts/keyspace.txt

Connectez-vous avec cqlsh et vérifiez que le keyspace a bien été créé:

    $ cqlsh
    $ DESC KEYSPACE workshop;
    $ exit

## Création des column families (Table)

Exécuter le script `column_families.txt` :

    $ cqlsh -k workshop < ./src/test/resources/scripts/column_families.txt

Connectez-vous avec cqlsh et vérifiez que les tables ont bien été créées:

    $ cqlsh
    $ USE workshop;
    $ DESC TABLES;
    $ exit

Sujet du Workshop
=================

Nous sommes une startup ambitieuse qui souhaite créer un spinoff de LastFM,
bien entendu supérieur.

Nous estimons que la demande nous apportera plusieurs millions d'utilisateurs
très rapidement. Nous avons donc choisi Cassandra, réputé pour ses performances
et sa scalabilité linéaire.

Le but de l'exercice est de faire passer les tests unitaires dans l'ordre des
méthodes de la classe  `CassandraRepositoryTest.java`.

La documentation du driver Datastax se trouve ici : http://www.datastax.com/doc-source/developer/java-driver/

### Exercice 1

Ecrire dans la table des users avec un statement statique.

### Exercice 2

Lire de la table des users

### Exercice 3

Insérer une track et sa collection de tags avec.

### Exercice 4

Ecrire dans la table du click stream avec le ttl précisé

### Exercice 5

Lire de la table des click streams entre 2 timestamps

### Exercice 6

Ecrire et des likes de user sur des tracks, de manière asynchrone

### Exercice 7

Spawner un second noeud, et écrire des users en mode batch

Bonus: Configurer un Cluster
=================

Pour avoir plus d'un noeud en local et pouvoir inspecter le ring, il faut installer des outils complémentaires.

### Sous Linux (Debian)

Installer Cassandra Cluser Manager

    % pip install ccm

### Sous OSX

Installer Cassandra Cluser Manager

    $ sudo port -v sync && sudo port -v install ccm

### Sous Windows

Non supporté pour le moment.

Pour plus d'informations : https://github.com/pcmanus/ccm


