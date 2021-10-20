package RDDAssignment

import java.math.BigInteger
import java.security.MessageDigest
import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats}

/**
 * Hint regarding the exercises: it is sometimes specified that the assignment asks about the committer or the
 * commit author. Those are two different entities, as per the Commit.scala file. Inspect it thoroughly and make
 * sure to always refer to the proper entity!
 */
object RDDAssignment {


  /**
   *                                     Description
   *
   * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
   * we want to know how many commits a given RDD contains.
   *
   *
   *                                        Hints
   *
   * You should be able to complete this assignment with using only one function. If in doubt, read the Spark RDD
   * documentation in detail: https://spark.apache.org/docs/2.4.3/api/scala/index.html#org.apache.spark.rdd.RDD
   *
   * @param commits RDD containing commit data.
   * @return Long indicating the number of commits in the given RDD.
   */
  def assignment_1(commits: RDD[Commit]): Long = {
    println(commits)
    commits.map(x => 1).reduce((a, b) => a + b);
  }

  /**
   *                                     Description
   *
   * We want to know what is the most popular email domain.
   * We require a RDD containing tuples of the used
   *        - email domain
   *        - combined with the number of occurrences.
   *
   *                                       Hints
   *
   * You should use the email of the author
   *
   * @param commits RDD containing commit data.
   * @return RDD containing tuples indicating the email domain (extension) and number of occurrences.
   */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = {
    return commits.map(c => c.commit.author.email)
      .distinct()
      .filter(email => email.nonEmpty && email.split("@").length > 1)
      .map(email => email.split('@')(1))
      .groupBy(identity)
      .map(x => (x._1, x._2.size.toLong))
  }

  /**
   *                                        Description
   *
   * Return a Tuple with :
   *      - filename
   *      - number of changes of the most frequently changed file
   * If there is no filename, use 'unknown'.
   *
   *                                           Hints
   *
   * Files in a directory must have unique names but can have the same name in different directories.
   * During refactoring, files can be moved between directories directories, resulting in the same file
   * having a different absolute path from a point in time. However, a directory can have more than
   * one file with the same name (but in different directories), so just taking the file name might be too lenient.
   * To simplify things, you may assume that an absolute path is sufficient to identify a file. To further simplify this,
   * use absolute filepath as filename.
   * @param commits RDD containing commit data.
   * @return A tuple containing the filename and number of changes.
   */
  def assignment_3(commits: RDD[Commit]): (String, Long) = {
    val res = commits.map(commit => commit.files
      .filter(file => file.filename.isDefined))
//      .foreach(x=>println(x))
//      .map(file => (file.filename.get, file.changes)))
//      .collect().flatten.maxBy(_._2)

    val filenames = res.map(r => r.map(file => (file.filename.get, file.changes)))
    val flatten = filenames.flatMap(x => x)

    // tuple of filename and changes
    val grouped  = flatten.groupByKey()
    // yea
    val sum = grouped.map(x=>(x._1, x._2.sum))

    sum.sortBy(_._2, false).first()

  }

  /**
   *                                        Description
   *
   * Some users on Github might be interested in their ranking in number of comments.
   * Return a RDD containing tuples of:
   *        - the rank (zero indexed) of a commit author
   *        - commit author's name
   *        - the sum of comments counts made by the commit author.
   *
   * As in general with performance rankings, a higher performance means a better
   * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
   * tie. For the lexicographical ordering, the size of the letters should not matter.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing the rank number, commit author names and number of comments of author in order.
   */
  def assignment_4(commits: RDD[Commit]): RDD[(Long, String, Long)] = {
    val res = commits.map(c => (c.commit.author.name, c.commit.comment_count))
      .aggregateByKey(0)(_+_.toInt, _+_)
      .sortBy(_._1.toLowerCase())
      .sortBy(_._2, false)
      .zipWithIndex.map(x => (x._1, x._2))
      .map(x =>(x._2, x._1._1, x._1._2.toLong))
    return res;
  }

  /**
   *                                        Description
   *
   * We want to know how stable and how widely used some programming languages are.
   * There are many ways to achieve that,but for the purpose of this exercise,
   * the measure we choose is how many additions, deletions and changes occur in each file extension.
   * We will provide a list of file extensions.
   *
   * We want an RDD of tuples containing :
   *        - file extension name
   *        - Stats object. (Stats object is only used for commits and single files only have additions,
   * deletions and changes value)
   *
   * we want you to compose the Stats object for each file with those values.
   *
   *                                           Hints
   *
   * The value of "changes" is the sum of additions and deletions, so it is an equivalent of the
   * "total" value in stats.
   *
   * @param commits RDD containing commit data.
   * @param fileExtensions List of String containing file extensions
   * @return RDD containing file extension and an aggregation of the committers' Stats.
   */
  def assignment_5(commits: RDD[Commit], fileExtensions: List[String]): RDD[(String, Stats)] = {
    val res = commits.map(commit => commit.files)
        .map(files => files
            .map(file => (file.filename.get
              .split('.')(file.filename.get.split('.').length-1), file.additions + file.deletions)).distinct
        .groupBy(_._1).mapValues(x => x.map(_._2).sum))
//        .filterKeys(ext => fileExtensions.contains(ext)
//      )).foreach(x=>println("FILENAME", x))
//          .groupBy()
//          .mapValues()
      .foreach(x=>println("FILENAME", x))


    return null
  }

  /**
   *                                        Description
   *
   * There are different types of people, those who own repositories, and those who make commits.
   * There are also people who do both.
   *
   * We require as output an RDD containing :
   *      - names of commit authors and repository owners that have both committed to repositories
   *      and own repositories in the given RDD.
   *
   * Note that the repository owner is contained within Github urls.
   *
   * @param commits RDD containing commit data.
   * @return RDD of Strings representing the author names that have both committed to and own repositories.
   */
  def assignment_6(commits: RDD[Commit]): RDD[String] = {


    return commits
      .map(commit => commit.commit.author.name)
      .intersection(commits.map(commit => commit.url.split('/')(4)));
  }


  /**                                       IMPORTANT NOTE!!!!!!
   *
   * * From here on, expensive functions on RDDs like groupBy are *NOT* allowed.
   * In real life wide dependency functions are performance killers, but luckily there are better performing
   * alternatives! Automatic graders will check computation history of returned RDDs.
   */


  /**
   *                                            Description
   *
   *
   * Sometimes developers make mistakes, sometimes they make many. One way of observing mistakes in commits is by
   * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
   * in a commit.
   *
   * Note that for a commit to be eligible for a 'commit streak', its message must start with `Revert`.
   * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
   * is not a 'revert streak'.
   *
   * Return a RDD containing tuples of
   *      - repository name (can be derived from the url)
   *      - average streak length computed over all commits.
   *
   * @param commits RDD containing commit data.
   * @return RDD of Tuple type containing a repository name and a double representing the average streak length.
   */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Double)] = {
      val res = commits.map(commit => (commit.url, commit.commit.message))
        .foreach(x => println(x))

    return null;

  }

  /**
   *
   *                                      Description
   *
   * We want to know the number of commits that are made by unique committers (represented by the field committer
   * in CommitData) in a given RDD. Besides the number of commits, we also want to know how many different
   * repositories the committers committed to. The repository name can be found in url.
   *
   * @param commits RDD containing commit data.
   * @return RDD of tuple containing committer name, list of repositories and
   * total number of commits committed to that repository.
   */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[String], Long)] = ???

  /**
   *                                       Description
   *
   * Return RDD of tuples containing
   *  - repository names
   *  - list of all commit authors of that repository (commit.author.name), with date of first commit.
   *
   *
   *                                          Hint
   * Use commit.author.date
   *
   * @param commits RDD containing commit data.
   * @return RDD containing the repository names, list of tuples of Timestamps and commit author names
   */
  def assignment_9(commits: RDD[Commit]): RDD[(String, Iterable[(Timestamp, String)])] = ???


  /**
   *                                             Description
   *
   * We want to know the committers that worked on a certain file to make an overview of every file in a repository.
   *
   * Create a tuple containing
   *  - file name
   *  - set of tuples with name of committers
   *  - Stat object representing the changes made to the file by each committer.
   *
   * @param commits RDD containing commit data.
   * @param repository String name of repository
   * @return RDD containing tuples representing a file name and a list of tuples of committer names and Stats object.
   */
  def assignment_10(commits: RDD[Commit], repository: String): RDD[(String, List[(String, Stats)])] = ???


  /**
    * BONUS ASSIGNMENT STARTS HERE
    *
    * Hashing function that computes the md5 hash from a String, which in terms returns a Long to act as a hashing
    * function for repository name and username.
    *
    * @param s String to be hashed, consecutively mapped to a Long.
    * @return Long representing the MSB from the inputted String.
    */
  def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
    * OPTIONAL EXERCISE - do not expect help from TA
    *
    * Create a bi-directional graph from committer to repositories. Use md5HashString function above to create unique
    * identifiers for creating a graph.
    *
    * As the real usage Sparks GraphX library is out of the scope of this course, we will not go further into this, but
    * this can be used for algorithms like PageRank, Hubs and Authorities, clique finding, etc.
    *
    * We expect a node for each repository and each committer (based on committer name), an edge from each
    * committer to repositories the committer has committed to.
    *
    * Look at documentation of Graph and Edge before starting this complementary exercise.
    * Your vertices should contain information about the type of node, a 'developer' or a 'repository' node.
    * Edges should only exist between repositories and committers.
    *
    * @param commits RDD containing commit data.
    * @return Graph representation of the commits as described above.
    */
  def bonus_assignment(commits: RDD[Commit]): Graph[(String, String), String] = ???
}
