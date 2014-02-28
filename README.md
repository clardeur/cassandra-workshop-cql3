![alt text](http://cassandra.apache.org/media/img/cassandra_logo.png "cassandra_logo")

Installation
=================

Téléchargement
------

Choisissez le repertoire où installer Cassandra. ( ex: `~` ou `/usr/local` )

    $ INSTALL_DIR=/usr/local

Télécharger la version 2.0.5 de Cassandra.

    $ curl http://apache.mirrors.multidist.eu/cassandra/2.0.5/apache-cassandra-2.0.5-bin.tar.gz | tar xz -C $INSTALL_DIR
    
Si le téléchargement est lent, vous pouvez utiliser un autre [mirroir](http://www.apache.org/dyn/closer.cgi?path=/cassandra/2.0.5/apache-cassandra-2.0.5-bin.tar.gz).
    
Configuration
------

Configurer la variable d'enviornnement du HOME de Cassandra.

    $ export CASSANDRA_HOME=$INSTALL_DIR/apache-cassandra-2.0.5
    
Changer le repertoire par défaut où sont enregistrés les data et les logs. **(non obligatoire)**

    $ sed -i.backup 's@/var/lib@/tmp@g' $CASSANDRA_HOME/conf/cassandra.yaml
    
Lancement
------
    
Démmarer Cassandra en foreground.

    $ $CASSANDRA_HOME/bin/cassandra -f
    
Pour le stopper, `Ctrl+C` ou en killant le processus.

Workshop
=================

Pitch
------

Nous sommes une startup ambitieuse qui souhaite créer un spinoff de LastFM,
bien entendu supérieur.

Nous estimons que la demande nous apportera plusieurs millions d'utilisateurs
très rapidement. Nous avons donc choisi Cassandra, réputé pour ses performances
et sa scalabilité linéaire.

Si cela n'est pas encore fait, clonez le repository.

    $   git clone https://github.com/clardeur/cassandra-workshop-cql3.git
    
Tables
------

Le Keyspace et les Tables seront initialisées automatiquement dans Cassandra, lors du lancement des tests.

Si vous souhaitez consulter les scripts de création, il s'agit des fichiers `create-tables.cql` et `create-keyspace.cql` qui se trouvent dans le repertoire `src/main/resources/script` du projet.

Pour visualiser le schéma des tables, utiliser l'outil `cqlsh` et tapez `help` pour avoir la liste des commandes.

    $ $CASSANDRA_HOME/bin/cqlsh
    > help
    
N'hésitez pas à abuser votre touche TAB pour l'autocomplétion.

Objectif
------

Le but de l'exercice est de faire passer les tests unitaires dans l'ordre des
méthodes de la classe `CassandraRepositoryTest.java`.

Quelques liens utiles:

- [La syntaxe de CQL3](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cqlCommandsTOC.html)
- [La documentation sur le Driver Java](http://www.datastax.com/documentation/developer/java-driver/1.0/index.html)


Bonus: Configurer un Cluster
=================

Former un cluster avec l'ensemble des machines et le visualiser avec nodetool, voir OpsCenter. 

