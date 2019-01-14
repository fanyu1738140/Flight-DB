import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Runs queries against a back-end database
 */
public class Query
{
  private String configFilename;
  private Properties configProps = new Properties();

  private String jSQLDriver;
  private String jSQLUrl;
  private String jSQLUser;
  private String jSQLPassword;
  private ArrayList<Itinerary> itineraries = new ArrayList<Itinerary>();;

  // DB Connection
  private Connection conn;

  // Logged In User
  private String username; // customer username is unique

  private static final int OFFSET = 18;

  // Canned queries

  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  private static final String CHECK_FLIGHT_CAPACITY2 = "SELECT capacity FROM Capacities WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement2;

  private static final String GET_USER = "SELECT * FROM Users WHERE username = ? AND pass = ?";
  private PreparedStatement getUserStatement;

  private static final String CREATE_USER = "INSERT INTO Users VALUES(?, ?, ?)";
  private PreparedStatement createUserStatement;

  private static final String CHECK_RESERVE = "SELECT * FROM Reservations WHERE usr = ?";
  private PreparedStatement checkReserveStatement;

  private static final String GET_CAPACITY = "SELECT * FROM Flights WHERE fid = ?";
  private PreparedStatement getCapacityStatement;

  private static final String GET_USER2 = "SELECT * FROM Users WHERE username = ? and pass = ?;";
  private PreparedStatement getUserStatement2;

  private static final String INSERT_CAPACITY = "INSERT INTO Capacities "
    + "SELECT F.fid, F.capacity "
    + "FROM Flights F "
    + "WHERE F.fid = ? "
    + "AND NOT EXISTS "
    + "(SELECT * FROM Capacities c WHERE c.fid = f.fid);";
  private PreparedStatement insertCapacityStatement;

  private static final String GET_RESERVATION_COUNT = "SELECT count FROM ReserveCount;";
  private PreparedStatement getReservationCountStatement;

  private static final String SET_RESERVATION_COUNT = "UPDATE ReserveCount SET count = ((SELECT count FROM ReserveCount) + 1);";
  private PreparedStatement setReservationCountStatement;

  private static final String INSERT_RESERVATION = "INSERT INTO Reservations VALUES (?,?,?,?,?,?,?);";
  private PreparedStatement insertReservationStatement;

  private static final String GET_RESERVATION = "SELECT * FROM Reservations WHERE usr = ?;";
  private PreparedStatement getReservationStatement;

  private static final String GET_ONE_RESERVATION = "SELECT * FROM Reservations WHERE rid = ? AND usr = ?;";
  private PreparedStatement getOneReservation;

  private static final String SET_CAPACITY = "UPDATE Capacities SET capacity = ((SELECT capacity FROM Capacities WHERE fid = ?) - 1) WHERE fid = ?";
  private PreparedStatement setCapacityStatement;

  private static final String ADD_CAPACITY = "UPDATE Capacities SET capacity = ((SELECT capacity FROM Capacities WHERE fid = ?) + 1) WHERE fid = ?";
  private PreparedStatement addCapacityStatement;

  private static final String ADD_MONEY = "UPDATE Users SET balance = ((SELECT balance FROM Users WHERE username = ?) + ?) WHERE username = ?";
  private PreparedStatement addMoneyStatament;

  private static final String SPEND_MONEY = "UPDATE Users SET balance = ((SELECT balance FROM Users WHERE username = ?) - ?) WHERE username = ?";
  private PreparedStatement spendMoneyStatament;

  private static final String GET_FLIGHT = "SELECT * FROM Flights WHERE fid = ?;";
  private PreparedStatement getFlightStatement;
  // transactions
  private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  private PreparedStatement beginTransactionStatement;

  private static final String COMMIT_SQL = "COMMIT TRANSACTION";
  private PreparedStatement commitTransactionStatement;

  private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
  private PreparedStatement rollbackTransactionStatement;

  private static final String GET_PAID_STATUS = "SELECT * FROM Reservations WHERE rid = ? AND usr = ?;";
  private PreparedStatement getPaidStatusStatement;

  private static final String SET_PAID_STATUS = "UPDATE Reservations SET paid = ? WHERE rid = ? AND usr = ?;";
  private PreparedStatement setPaidStatusStatement;

  private static final String DELETE_RESERVATION = "DELETE FROM Reservations WHERE rid = ?;";
  private PreparedStatement deleteReservationStatement;

  private static final String GET_BALANCE = "SELECT balance FROM Users WHERE username = ?;";
  private PreparedStatement getBalanceStatement;

  private static final String DIRECT_SEARCH = "SELECT Top(?) * "
                                            + "FROM Flights "
                                            + "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? "
                                            + "AND canceled != 1 ORDER BY actual_time ASC, fid ASC;";
  private PreparedStatement directSearchStatement;

  private static final String INDIRECT_SEARCH = "SELECT TOP (?) * FROM Flights F1, Flights F2 "
                                              + "WHERE F1.origin_city = ? AND F1.dest_city = F2.origin_city AND F2.dest_city = ? "                                              + "AND F1.day_of_month = ? AND F1.day_of_month = F2.day_of_month "
                                              + "AND F1.canceled != 1 AND F2.canceled != 1 "
                                              + "ORDER BY (F1.actual_time + F2.actual_time), F1.fid ASC;";
  private PreparedStatement indirectSearchStatement;



  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    @Override
    public String toString()
    {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }

  class Itinerary implements Comparable<Itinerary>{
    public Flight f1;
    public Flight f2;
    public int dayOfMonth;
    public int cost;

    public int compareTo(Itinerary other)
    {
      int time1;
      int time2;
      if (this.f2 != null){
        time1 = this.f1.time + this.f2.time;
      } else  {
        time1 = this.f1.time;
      }
      if (other.f2 != null){
        time2 = other.f1.time + other.f2.time;
      } else  {
        time2 = other.f1.time;
      }

      return time1 - time2;
    }

  }

  public Query(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /* Connection code to SQL Azure.  */
  public void openConnection() throws Exception
  {
    configProps.load(new FileInputStream(configFilename));

    jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    jSQLUrl = configProps.getProperty("flightservice.url");
    jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement

    /* You will also want to appropriately set the transaction's isolation level through:
       conn.setTransactionIsolation(...)
       See Connection class' JavaDoc for details.
    */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created. Do not drop any tables and do not
   * clear the flights table. You should clear any tables you use to store reservations
   * and reset the next reservation ID to be 1.
   */
  public void clearTables ()
  {
    try {
      beginTransaction();
      Statement clear = conn.createStatement();
      clear.executeUpdate("DELETE FROM Reservations");
      clear.executeUpdate("DELETE FROM Users");
      clear.executeUpdate("DELETE FROM Capacities");
      clear.executeUpdate("DELETE FROM ReserveCount");
      commitTransaction();
    } catch (SQLException e) { 
      e.printStackTrace(); 
    }
  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
    commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
    rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

    directSearchStatement = conn.prepareStatement(DIRECT_SEARCH);
    indirectSearchStatement = conn.prepareStatement(INDIRECT_SEARCH);
    getUserStatement = conn.prepareStatement(GET_USER);
    createUserStatement = conn.prepareStatement(CREATE_USER);
    checkReserveStatement = conn.prepareStatement(CHECK_RESERVE);
    getCapacityStatement = conn.prepareStatement(GET_CAPACITY);
    setCapacityStatement = conn.prepareStatement(SET_CAPACITY);
    insertCapacityStatement = conn.prepareStatement(INSERT_CAPACITY);
    getReservationCountStatement = conn.prepareStatement(GET_RESERVATION_COUNT);
    setReservationCountStatement = conn.prepareStatement(SET_RESERVATION_COUNT);
    insertReservationStatement = conn.prepareStatement(INSERT_RESERVATION);
    getReservationStatement = conn.prepareStatement(GET_RESERVATION);
    getFlightStatement = conn.prepareStatement(GET_FLIGHT);
    getOneReservation = conn.prepareStatement(GET_ONE_RESERVATION);
    addCapacityStatement = conn.prepareStatement(ADD_CAPACITY);
    addMoneyStatament = conn.prepareStatement(ADD_MONEY);
    spendMoneyStatament = conn.prepareStatement(SPEND_MONEY);
    getPaidStatusStatement = conn.prepareStatement(GET_PAID_STATUS);
    setPaidStatusStatement = conn.prepareStatement(SET_PAID_STATUS);
    deleteReservationStatement = conn.prepareStatement(DELETE_RESERVATION);
    getBalanceStatement = conn.prepareStatement(GET_BALANCE);
    getUserStatement2 = conn.prepareStatement(GET_USER2);
    checkFlightCapacityStatement2 = conn.prepareStatement(CHECK_FLIGHT_CAPACITY2);
    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username
   * @param password
   *
   * @return If someone has already logged in, then return "User already logged in\n"
   * For all other errors, return "Login failed\n".
   *
   * Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password)
  {
    if (this.username != null) {
      return "User already logged in\n";
    } else {
      try {
        beginTransaction();

        getUserStatement.clearParameters();
        getUserStatement.setString(1, username);
        getUserStatement.setString(2, password);
        ResultSet results = getUserStatement.executeQuery();
        if (results.next()) {
          this.username = username;
          results.close();
          commitTransaction();
          return "Logged in as " + username + "\n"; 
        }
        rollbackTransaction();
        results.close();
        return "Login failed\n";
      } catch (SQLException e) {
        e.printStackTrace();
        return "Login failed\n";
      }
    }
  }

  /**
   * Implement the create user function.
   *
   * @param username new user's username. User names are unique the system.
   * @param password new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer (String username, String password, int initAmount)
  {
    //check if user DOES NOT exist
    //then create customer
    if (initAmount >=0){
      try{
        beginTransaction();
        getUserStatement2.clearParameters();
        getUserStatement2.setString(1,username);
        getUserStatement2.setString(2,password);
        ResultSet re = getUserStatement2.executeQuery();
        //THERE ALREADY EXISTS A USER W SAME USERNAME
        if (re.next()){
          re.close();
          rollbackTransaction();
          return "Failed to create user\n";
        }
        createUserStatement.clearParameters();
        createUserStatement.setString(1,username);
        createUserStatement.setString(2,password);
        createUserStatement.setInt(3,initAmount);
        createUserStatement.execute();
        commitTransaction();
        return "Created user " + username + "\n";
        } catch (SQLException e) {
        //e.printStackTrace();
      }
      }
      return "Failed to create user\n";
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
    StringBuffer sb = new StringBuffer();
    this.itineraries = new ArrayList<Itinerary>();
    int numItineratries = numberOfItineraries;
    int count = 0;

    try{
      beginTransaction();
      // direct flight data 
      directSearchStatement.clearParameters();
      directSearchStatement.setInt(1, numItineratries);
      directSearchStatement.setString(2, originCity);
      directSearchStatement.setString(3, destinationCity);
      directSearchStatement.setInt(4, dayOfMonth);

      ResultSet oneHopResults = directSearchStatement.executeQuery();

      while (oneHopResults.next() && numItineratries > 0) {
        Flight flight = parser(oneHopResults, true);
        //sb.append("Itinerary " + count + ": 1 flight(s), "+ flight.time +" minutes\n");
        //sb.append(flight.toString() + "\n");
        Itinerary it = new Itinerary();
        it.f1 = flight;
        it.f2 = null;
        it.dayOfMonth = flight.dayOfMonth;
        it.cost = flight.price;
        this.itineraries.add(it);
        numItineratries --;
        count ++;
      } 
      oneHopResults.close();

      if (numItineratries > 0 && !directFlight) {
        indirectSearchStatement.clearParameters();
        indirectSearchStatement.setInt(1, numItineratries);
        indirectSearchStatement.setString(2, originCity);
        indirectSearchStatement.setString(3, destinationCity);
        indirectSearchStatement.setInt(4, dayOfMonth);

        ResultSet twoHopResults = indirectSearchStatement.executeQuery();

        while (twoHopResults.next() && numItineratries > 0){
          Flight flight1 = parser(twoHopResults, true);
          Flight flight2 = parser(twoHopResults, false);
          //int totalTime = flight1.time + flight2.time;

          Itinerary it = new Itinerary();
          it.f1 = flight1;
          it.f2 = flight2;
          it.dayOfMonth = flight1.dayOfMonth; 
          it.cost = flight1.price + flight2.price;
          this.itineraries.add(it);

          //sb.append("Itinerary " + count + ": 2 flight(s), "+ totalTime +" minutes\n");
          //sb.append(flight1.toString() + "\n");
          //sb.append(flight2.toString() + "\n");
          numItineratries --;
          count ++;
        }
        twoHopResults.close();
      }
      Collections.sort(this.itineraries);
      for (int i = 0; i < this.itineraries.size(); i ++) {
        Itinerary out = this.itineraries.get(i);
        if (out.f2 != null) {
          sb.append("Itinerary " + i + ": 2 flight(s), "+ (out.f1.time + out.f2.time) +" minutes\n");
          sb.append(out.f1.toString() + "\n");
          sb.append(out.f2.toString() + "\n");
        } else  {
          sb.append("Itinerary " + i + ": 1 flight(s), "+ out.f1.time +" minutes\n");
          sb.append(out.f1.toString() + "\n");
        }
      }
      commitTransaction();
    } catch (SQLException e) { 
      // e.printStackTrace();
    }

    if (count == 0) {
      return "No flights match your selection\n";
    }
    // sort this.itineraries then create out put

    return sb.toString();
    //return transaction_search_unsafe(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
  }

  private Flight parser(ResultSet results, boolean direct) throws SQLException {
    int offset = 0;
    if (!direct){
      offset = this.OFFSET;
    }
    Flight flight = new Flight();
    flight.fid = results.getInt(1 + offset);
    flight.dayOfMonth = results.getInt(3 + offset);
    flight.carrierId = results.getString(5 + offset);
    flight.flightNum = results.getString(6 + offset);
    flight.originCity = results.getString(7 + offset);
    flight.destCity = results.getString(9 + offset);
    flight.time = results.getInt(15 + offset);
    flight.capacity = results.getInt(17 + offset);
    flight.price = results.getInt(18 + offset);
    return flight;
  }

  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
                                          int dayOfMonth, int numberOfItineraries)
  {
    StringBuffer sb = new StringBuffer();

    try
    {
      // one hop itineraries
      String unsafeSearchSQL =
              "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
                      + "FROM Flights "
                      + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
                      + "ORDER BY actual_time ASC";

      Statement searchStatement = conn.createStatement();
      ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

      while (oneHopResults.next())
      {
        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
        String result_carrierId = oneHopResults.getString("carrier_id");
        String result_flightNum = oneHopResults.getString("flight_num");
        String result_originCity = oneHopResults.getString("origin_city");
        String result_destCity = oneHopResults.getString("dest_city");
        int result_time = oneHopResults.getInt("actual_time");
        int result_capacity = oneHopResults.getInt("capacity");
        int result_price = oneHopResults.getInt("price");

        sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
      }
      oneHopResults.close();
    } catch (SQLException e) { 
      //e.printStackTrace(); 
    }

    return sb.toString();
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
   * If the user already has a reservation on the same day as the one that they are trying to book now, then return
   * "You cannot book two flights in the same day\n".
   * For all other errors, return "Booking failed\n".
   *
   * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
   * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
   * successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId)
  {
    if(this.username == null)
      return "Cannot book reservations, not logged in\n";

    if(itineraryId < 0 || itineraryId >= this.itineraries.size())
      return "No such itinerary "+ itineraryId +"\n";

    Itinerary it = this.itineraries.get(itineraryId);

    try {
      beginTransaction();
      checkReserveStatement.clearParameters();
      checkReserveStatement.setString(1, this.username);
      ResultSet result = checkReserveStatement.executeQuery();
      while (result.next()) {
        int bookedDay = result.getInt("day");

        if (it.dayOfMonth == bookedDay) {
          result.close();
          rollbackTransaction();
          return "You cannot book two flights in the same day\n";
        }
      }
    
      result.close();

      // check capacity of flight 1
      int capacity1;
      checkFlightCapacityStatement2.clearParameters();
      checkFlightCapacityStatement2.setInt(1, it.f1.fid);
      ResultSet re = checkFlightCapacityStatement2.executeQuery();
      if (re.isBeforeFirst()) {
        re.next();
        capacity1 = re.getInt("capacity");
      } else {
        insertCapacityStatement.clearParameters();
        insertCapacityStatement.setInt(1, it.f1.fid);
        insertCapacityStatement.executeUpdate();
        getCapacityStatement.clearParameters();
        getCapacityStatement.setInt(1, it.f1.fid);
        ResultSet capacityresult = getCapacityStatement.executeQuery();
        capacityresult.next();
        capacity1 = capacityresult.getInt("capacity");
        capacityresult.close();
      }
      re.close();

      if (capacity1 == 0) {
        rollbackTransaction();
        return "Booking failed\n";
      }

      // check capacity of flight 2 
      if (it.f2 != null) {
        int capacity2;
        checkFlightCapacityStatement2.clearParameters();
        checkFlightCapacityStatement2.setInt(1, it.f2.fid);
        ResultSet re2 = checkFlightCapacityStatement2.executeQuery();
        if (re2.isBeforeFirst()) {
          re2.next();
          capacity2 = re2.getInt("capacity");
        } else {
          insertCapacityStatement.clearParameters();
          insertCapacityStatement.setInt(1, it.f2.fid);
          insertCapacityStatement.executeUpdate();
          getCapacityStatement.clearParameters();
          getCapacityStatement.setInt(1, it.f2.fid);
          ResultSet capacityresult2 = getCapacityStatement.executeQuery();
          capacityresult2.next();
          capacity2 = capacityresult2.getInt("capacity");
          capacityresult2.close();
        }
        re2.close();

        if (capacity2 == 0) {
          rollbackTransaction();
          return "Booking failed\n";
        }
      }

      // update capacity 

      setCapacityStatement.clearParameters();
      setCapacityStatement.setInt(1, it.f1.fid);
      setCapacityStatement.setInt(2, it.f1.fid);
      setCapacityStatement.executeUpdate();

      if (it.f2 != null) {
        setCapacityStatement.clearParameters();
        setCapacityStatement.setInt(1, it.f2.fid);
        setCapacityStatement.setInt(2, it.f2.fid);
        setCapacityStatement.executeUpdate();
      }

      // update reservation
      getReservationCountStatement.clearParameters();
      ResultSet reserveCount = getReservationCountStatement.executeQuery();
      int count;
      if (reserveCount.isBeforeFirst()) {
        reserveCount.next();
        count = reserveCount.getInt("count");
      } else {
        Statement ini = conn.createStatement();
        ini.executeUpdate("INSERT INTO ReserveCount VALUES (0)");
        count = 0;
      }

      reserveCount.close();
      setReservationCountStatement.clearParameters();
      setReservationCountStatement.executeUpdate();

      insertReservationStatement.clearParameters();
      insertReservationStatement.setInt(1, count+1);
      insertReservationStatement.setInt(2, it.f1.fid);
      if (it.f2 != null)
        insertReservationStatement.setInt(3, it.f2.fid);
      else 
        insertReservationStatement.setInt(3, -1);
      insertReservationStatement.setString(4, this.username);
      insertReservationStatement.setInt(5, 0);
      insertReservationStatement.setInt(6, it.cost);
      insertReservationStatement.setInt(7, it.f1.dayOfMonth);
      insertReservationStatement.executeUpdate();
      commitTransaction();
      return "Booked flight(s), reservation ID: " + (count+1) + "\n";
    } catch (SQLException e) {
      // e.printStackTrace();
      return "Booking failed\n";
    }
  }
  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
   * If the user has no reservations, then return "No reservations found\n"
   * For all other errors, return "Failed to retrieve reservations\n"
   *
   * Otherwise return the reservations in the following format:
   *
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * ...
   *
   * Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations()
  {
    if (this.username == null) {
      return "Cannot view reservations, not logged in\n";
    }
    try {
      beginTransaction();
      getReservationStatement.clearParameters();
      getReservationStatement.setString(1, this.username);
      ResultSet resever = getReservationStatement.executeQuery();
      if (resever.isBeforeFirst()) {
        StringBuffer sb = new StringBuffer();
        while(resever.next()) {
          int rid = resever.getInt("rid");
          int pid = resever.getInt("paid");
          int fid1 = resever.getInt("fid1");
          int fid2 = resever.getInt("fid2");
          String paid;
          if (pid == 1)
            paid = "true";
          else
            paid = "false";
          sb.append("Reservation "+ rid +" paid: " + paid + ":\n");
          sb.append(flightinfo(fid1)+ "\n");
          if (fid2 != -1)
            sb.append(flightinfo(fid2)+ "\n");
        }
        return sb.toString();
      } else {
        resever.close();
        commitTransaction();
        return "No reservations found\n";
      }
    } catch (SQLException e) {
      // e.printStackTrace();
      return "Failed to retrieve reservations\n";
    }
  }

  private String flightinfo(int fid) throws SQLException{
    beginTransaction();
    getFlightStatement.clearParameters();
    getFlightStatement.setInt(1, fid);
    ResultSet re = getFlightStatement.executeQuery();
    re.next();
    Flight f = parser(re, true);
    re.close();
    commitTransaction();
    return f.toString();
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
   * For all other errors, return "Failed to cancel reservation [reservationId]"
   *
   * If successful, return "Canceled reservation [reservationId]"
   *
   * Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId)
  {
    // only implement this if you are interested in earning extra credit for the HW!
    if (this.username == null) {
      return "Cannot cancel reservations, not logged in\n";
    }
    try {
      beginTransaction();
      getOneReservation.clearParameters();
      getOneReservation.setInt(1, reservationId);
      getOneReservation.setString(2, this.username);
      ResultSet re = getOneReservation.executeQuery();
      if (re.isBeforeFirst()) {
        re.next();
        int pid = re.getInt("paid");
        int fid1 = re.getInt("fid1");
        int fid2 = re.getInt("fid2");

        // add capacity
        addCapacityStatement.clearParameters();
        addCapacityStatement.setInt(1, fid1);
        addCapacityStatement.setInt(2, fid1);
        addCapacityStatement.executeUpdate();
        if (fid2 != -1) {
          addCapacityStatement.clearParameters();
          addCapacityStatement.setInt(1, fid2);
          addCapacityStatement.setInt(2, fid2);
          addCapacityStatement.executeUpdate();
        }
        // refund
        if (pid == 1) {
          int money = re.getInt("cost");
          addMoneyStatament.clearParameters();
          addMoneyStatament.setString(1, this.username);
          addMoneyStatament.setInt(2, money);
          addMoneyStatament.setString(3, this.username);
          addMoneyStatament.executeUpdate();
        }
        re.close();
        // delete reservation
        deleteReservationStatement.clearParameters();
        deleteReservationStatement.setInt(1, reservationId);
        deleteReservationStatement.executeUpdate();
        commitTransaction();
        return "Canceled reservation " + reservationId + "\n";
      }
      return "Failed to cancel reservation " + reservationId + "\n";
    } catch (SQLException e) {
      // e.printStackTrace();
      return "Failed to cancel reservation " + reservationId + "\n";
    }
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   * If the reservation is not found / not under the logged in user's name, then return
   * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
   * If the user does not have enough money in their account, then return
   * "User has only [balance] in account but itinerary costs [cost]\n"
   * For all other errors, return "Failed to pay for reservation [reservationId]\n"
   *
   * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
   * where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay (int reservationId)
  {
    if (this.username == null){
      return "Cannot pay, not logged in\n";
    }
    try {
      beginTransaction();
      getPaidStatusStatement.clearParameters();
      getPaidStatusStatement.setInt(1, reservationId);
      getPaidStatusStatement.setString(2, this.username);

      ResultSet p = getPaidStatusStatement.executeQuery();
      if (!p.isBeforeFirst()) {
        return "Cannot find unpaid reservation " + reservationId + " under user: " + this.username + "\n";
      } else {
        p.next();
        int paid = p.getInt("paid");
        if (paid == 1) {
          p.close();
          rollbackTransaction();
          return "Cannot find unpaid reservation " + reservationId + " under user: " + this.username + "\n";
        } 

        int cost = p.getInt("cost");

        getBalanceStatement.clearParameters();
        getBalanceStatement.setString(1, this.username);
        ResultSet re = getBalanceStatement.executeQuery();
        re.next();
        int balance = re.getInt("balance");
        if (cost > balance) {
          p.close();
          re.close();
          rollbackTransaction();
          return "User has only " + balance + " in account but itinerary costs " + cost + "\n";
        } else {
          spendMoneyStatament.clearParameters();
          spendMoneyStatament.setString(1, this.username);
          spendMoneyStatament.setInt(2, cost);
          spendMoneyStatament.setString(3, this.username);
          spendMoneyStatament.executeUpdate();

          setPaidStatusStatement.clearParameters();
          setPaidStatusStatement.setInt(1, 1);
          setPaidStatusStatement.setInt(2, reservationId);
          setPaidStatusStatement.setString(3, this.username);
          setPaidStatusStatement.executeUpdate();
          commitTransaction();
          return "Paid reservation: " + reservationId + " remaining balance: " + (balance - cost) + "\n";
        }
      }
    } catch (SQLException e) { 
      // e.printStackTrace(); 
    } 
    return "Failed to pay for reservation " + reservationId + "\n";
  }

  /* some utility functions below */

  public void beginTransaction() throws SQLException
  {
    conn.setAutoCommit(false);
    beginTransactionStatement.executeUpdate();
  }

  public void commitTransaction() throws SQLException
  {
    commitTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  public void rollbackTransaction() throws SQLException
  {
    rollbackTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments. You don't need to
   * use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }
}
