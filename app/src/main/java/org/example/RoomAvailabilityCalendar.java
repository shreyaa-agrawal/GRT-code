package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * RoomAvailabilityCalendar
 *
 * @tagline Java 24 program to manage room availability calendar –
 *           reading CSV bookings, maintenance, linked rooms, avoiding overlap,
 *           outputting JSON, masking sensitive info in logs.
 *
 * @intuition Parse input CSV files into domain records (Booking, Maintenance, Rooms),
 *            then merge occupancies to derive available dates per room considering linked suites.
 *
 * @approach Immutable records represent bookings and maintenance blocking room/date spansd interval trees (via sorted lists) to detect by room and linked rooms for availability;
 *           Mask sensitive fields when logging;
 *           Defensive input validation and robust error handling;
 *           Output full availability calendar in JSON format using pure Java.
 *
 * @complexity
 *   Time: O(N log N) where N is total events (bookings+maintenance);
 *   Space: O(N) for storage of input data and availability mappings.
 */
public final class RoomAvailabilityCalendar {
  private static final Logger LOG = Logger.getLogger(RoomAvailabilityCalendar.class.getName());
  private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ISO_LOCAL_DATE;

  // Records capture domain objects: only non-sensitive info stored
  public record Booking(String maskedGuestId, String roomId, LocalDate from, LocalDate to) {}
  public record Maintenance(String roomId, LocalDate from, LocalDate to) {}
  public record LinkedRoomType(String suiteId, List<String> rooms) {}

  public record DateRange(LocalDate start, LocalDate end) {
    public boolean overlaps(DateRange other) {
      return !(end.isBefore(other.start) || start.isAfter(other.end));
    }
  }

  /**
   * Parses a CSV file to bookings.
   * Expected columns: guest_id,email,room_id,from_date,to_date
   * Logs masked guest info to avoid PCI DSS violation.
   */
  private static List<Booking> parseBookings(Reader csvReader) throws IOException {
    try (var br = new BufferedReader(csvReader)) {
      String header = br.readLine();
      if (header == null) throw new IOException("Empty bookings CSV");

      String[] columns = header.split(",");
      if (columns.length < 5 || !columns[0].equalsIgnoreCase("guest_id") || !columns[2].equalsIgnoreCase("room_id"))
        throw new IOException("Bookings CSV header invalid");

      List<Booking> bookings = new ArrayList<>();
      String line;
      int lineNum = 1;
      while ((line = br.readLine()) != null) {
        lineNum++;
        if (line.trim().isEmpty()) continue; // Skip empty lines
        
        String[] fields = line.split(",");
        if (fields.length < 5) {
          if (LOG.isLoggable(Level.WARNING)) {
            LOG.warning(String.format("Skipping incomplete booking line %d", lineNum));
          }
          continue;
        }
        try {
          String guestRaw = fields[0].trim();
          String emailRaw = fields[1].trim();
          final var maskedGuestId = maskGuestId(guestRaw);
          final var maskedEmail = maskEmail(emailRaw);
          // Mask in logs only; no storing sensitive field beyond masked guest id
          if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("Booking Guest=%s Email=%s Room=%s From=%s To=%s",
                    maskedGuestId, maskedEmail, fields[2], fields[3], fields[4]));
          }

          String roomId = fields[2].trim();
          LocalDate from = LocalDate.parse(fields[3].trim(), DT_FMT);
          LocalDate to = LocalDate.parse(fields[4].trim(), DT_FMT);
          if (from.isAfter(to))
            throw new IllegalArgumentException("Booking 'from' date after 'to' date");

          bookings.add(new Booking(maskedGuestId, roomId, from, to));
        } catch (Exception e) {
          LOG.log(Level.WARNING, "Skipping invalid booking line " + lineNum + ": " + e.getMessage());
        }
      }
      return bookings;
    }
  }

  /**
   * Parses a CSV file to maintenance periods.
   * Expected columns: room_id,from_date,to_date
   */
  private static List<Maintenance> parseMaintenance(Reader csvReader) throws IOException {
    try (var br = new BufferedReader(csvReader)) {
      String header = br.readLine();
      if (header == null) throw new IOException("Empty maintenance CSV");

      String[] columns = header.split(",");
      if (columns.length < 3 || !columns[0].equalsIgnoreCase("room_id"))
        throw new IOException("Maintenance CSV header invalid");

      List<Maintenance> maintenance = new ArrayList<>();
      String line;
      int lineNum = 1;
      while ((line = br.readLine()) != null) {
        lineNum++;
        if (line.trim().isEmpty()) continue; // Skip empty lines
        
        String[] fields = line.split(",");
        if (fields.length < 3) {
          if (LOG.isLoggable(Level.WARNING)) {
            LOG.warning(String.format("Skipping incomplete maintenance line %d", lineNum));
          }
          continue;
        }
        try {
          String roomId = fields[0].trim();
          LocalDate from = LocalDate.parse(fields[1].trim(), DT_FMT);
          LocalDate to = LocalDate.parse(fields[2].trim(), DT_FMT);
          if (from.isAfter(to))
            throw new IllegalArgumentException("Maintenance 'from' date after 'to' date");
          maintenance.add(new Maintenance(roomId, from, to));
        } catch (Exception e) {
          LOG.log(Level.WARNING, "Skipping invalid maintenance line " + lineNum + ": " + e.getMessage());
        }
      }
      return maintenance;
    }
  }

  /**
   * Parses a CSV to linked room types (e.g., suites).
   * Expected columns: suite_id,room_id1,room_id2,room_id3,...
   * Each row has suite_id in first column, followed by room_ids in subsequent columns
   */
  private static List<LinkedRoomType> parseLinkedRooms(Reader csvReader) throws IOException {
    try (var br = new BufferedReader(csvReader)) {
      String header = br.readLine();
      if (header == null) throw new IOException("Empty linked rooms CSV");

      String[] columns = header.split(",");
      if (columns.length < 2 || !columns[0].equalsIgnoreCase("suite_id"))
        throw new IOException("Linked rooms CSV header invalid");

      List<LinkedRoomType> linkedRooms = new ArrayList<>();
      String line;
      int lineNum = 1;
      while ((line = br.readLine()) != null) {
        lineNum++;
        if (line.trim().isEmpty()) continue; // Skip empty lines
        
        String[] fields = line.split(",");
        if (fields.length < 2) {
          if (LOG.isLoggable(Level.WARNING)) {
            LOG.warning(String.format("Skipping incomplete linked rooms line %d", lineNum));
          }
          continue;
        }
        try {
          String suiteId = fields[0].trim();
          // All columns after the first one are room IDs
          List<String> rooms = new ArrayList<>();
          for (int i = 1; i < fields.length; i++) {
            String roomId = fields[i].trim();
            if (!roomId.isEmpty()) {
              rooms.add(roomId);
            }
          }
          
          if (rooms.isEmpty())
            throw new IllegalArgumentException("No rooms specified for suite");

          linkedRooms.add(new LinkedRoomType(suiteId, rooms));
        } catch (Exception e) {
          LOG.log(Level.WARNING, "Skipping invalid linked rooms line " + lineNum + ": " + e.getMessage());
        }
      }
      return linkedRooms;
    }
  }

  /**
   * Masks sensitive guest ID by hashing or redacting.
   * Here: simple masking exposing first and last char only.
   */
  public static String maskGuestId(String rawId) {
    if (rawId == null || rawId.isEmpty()) return "*****";
    if (rawId.length() <= 2) return "*".repeat(rawId.length());
    return rawId.charAt(0) + "*".repeat(rawId.length() - 2) + rawId.charAt(rawId.length() - 1);
  }

  /**
   * Masks email by exposing domain only with prefix redacted.
   */
  public static String maskEmail(String rawEmail) {
    if (rawEmail == null || rawEmail.isBlank()) return "***";
    int at = rawEmail.indexOf('@');
    if (at < 1) return "***";
    return "***" + rawEmail.substring(at);
  }

  /**
   * Main logic: for each room, combine booking and maintenance periods to find occupied spans,
   * then calculate availability as gaps between occupied spans within the global date range.
   * Linked rooms (suites) availability is intersection of constituent rooms availability.
   */
  public static Map<String, List<DateRange>> calculateAvailability(
          List<Booking> bookings,
          List<Maintenance> maintenance,
          List<LinkedRoomType> linkedRooms) {

    Set<String> allRooms = collectAllRooms(bookings, maintenance, linkedRooms);
    if (allRooms.isEmpty()) return Collections.emptyMap();

    LocalDate minDate = findMinDate(bookings, maintenance);
    LocalDate maxDate = findMaxDate(bookings, maintenance, minDate);

    Map<String, List<DateRange>> occupiedMap = buildOccupiedMap(allRooms, bookings, maintenance);
    Map<String, List<DateRange>> availability = calculateRoomAvailability(occupiedMap, minDate, maxDate);
    addLinkedRoomAvailability(availability, linkedRooms);

    return availability;
  }

  private static Set<String> collectAllRooms(List<Booking> bookings, List<Maintenance> maintenance, List<LinkedRoomType> linkedRooms) {
    Set<String> allRooms = new HashSet<>();
    bookings.forEach(b -> allRooms.add(b.roomId()));
    maintenance.forEach(m -> allRooms.add(m.roomId()));
    linkedRooms.forEach(lt -> allRooms.addAll(lt.rooms()));
    return allRooms;
  }

  private static LocalDate findMinDate(List<Booking> bookings, List<Maintenance> maintenance) {
    return Stream.concat(
            bookings.stream().map(Booking::from),
            maintenance.stream().map(Maintenance::from))
        .min(LocalDate::compareTo).orElse(LocalDate.now());
  }

  private static LocalDate findMaxDate(List<Booking> bookings, List<Maintenance> maintenance, LocalDate minDate) {
    return Stream.concat(
            bookings.stream().map(Booking::to),
            maintenance.stream().map(Maintenance::to))
        .max(LocalDate::compareTo).orElse(minDate.plusMonths(3));
  }

  private static Map<String, List<DateRange>> buildOccupiedMap(Set<String> allRooms, List<Booking> bookings, List<Maintenance> maintenance) {
    Map<String, List<DateRange>> occupiedMap = new HashMap<>();
    allRooms.forEach(room -> occupiedMap.put(room, new ArrayList<>()));

    for (var b : bookings) {
      var range = new DateRange(b.from, b.to);
      List<DateRange> occupied = occupiedMap.get(b.roomId());
      if (occupied == null) {
        LOG.log(Level.WARNING, "Unknown room in booking: " + b.roomId());
        continue;
      }
      if (hasOverlap(occupied, range)) {
        throw new IllegalStateException("Overlapping booking detected for room " + b.roomId() +
            " in range " + b.from + " to " + b.to);
      }
      occupied.add(range);
    }
    for (var m : maintenance) {
      var range = new DateRange(m.from, m.to);
      List<DateRange> occupied = occupiedMap.get(m.roomId());
      if (occupied == null) {
        LOG.log(Level.WARNING, "Unknown room in maintenance: " + m.roomId());
        continue;
      }
      occupied.add(range);
    }
    occupiedMap.values().forEach(list -> list.sort(Comparator.comparing(r -> r.start)));
    occupiedMap.replaceAll((_, intervals) -> mergeOverlaps(intervals));
    return occupiedMap;
  }

  private static Map<String, List<DateRange>> calculateRoomAvailability(Map<String, List<DateRange>> occupiedMap, LocalDate minDate, LocalDate maxDate) {
    Map<String, List<DateRange>> availability = new HashMap<>();
    for (var entry : occupiedMap.entrySet()) {
      final var room = entry.getKey();
      final var occ = entry.getValue();
      final List<DateRange> free = new ArrayList<>();

      LocalDate cursor = minDate;
      for (DateRange occRange : occ) {
        if (cursor.isBefore(occRange.start))
          free.add(new DateRange(cursor, occRange.start.minusDays(1)));
        cursor = occRange.end.plusDays(1);
      }
      if (!cursor.isAfter(maxDate))
        free.add(new DateRange(cursor, maxDate));
      availability.put(room, free);
    }
    return availability;
  }

  private static void addLinkedRoomAvailability(Map<String, List<DateRange>> availability, List<LinkedRoomType> linkedRooms) {
    for (var suite : linkedRooms) {
      if (suite.rooms().isEmpty()) continue;
      List<DateRange> intersection = availability.getOrDefault(suite.rooms().get(0), List.of());

      for (int i = 1; i < suite.rooms().size(); i++) {
        var roomAvail = availability.getOrDefault(suite.rooms().get(i), List.of());
        intersection = intersectRanges(intersection, roomAvail);
      }
      availability.put(suite.suiteId(), intersection);
    }
  }

  private static boolean hasOverlap(List<DateRange> intervals, DateRange newRange) {
    for (var r : intervals) if (r.overlaps(newRange)) return true;
    return false;
  }

  private static List<DateRange> mergeOverlaps(List<DateRange> intervals) {
    if (intervals.isEmpty()) return intervals;
    List<DateRange> merged = new ArrayList<>();
    DateRange current = intervals.get(0);
    for (int i = 1; i < intervals.size(); i++) {
      DateRange next = intervals.get(i);
      if (!current.end.plusDays(1).isBefore(next.start)) {
        current = new DateRange(current.start, current.end.isAfter(next.end) ? current.end : next.end);
      } else {
        merged.add(current);
        current = next;
      }
    }
    merged.add(current);
    return merged;
  }

  private static List<DateRange> intersectRanges(List<DateRange> a, List<DateRange> b) {
    List<DateRange> result = new ArrayList<>();
    int i = 0;
    int j = 0;
    while (i < a.size() && j < b.size()) {
      var r1 = a.get(i);
      var r2 = b.get(j);
      LocalDate start = r1.start.isAfter(r2.start) ? r1.start : r2.start;
      LocalDate end = r1.end.isBefore(r2.end) ? r1.end : r2.end;
      if (!start.isAfter(end)) {
        result.add(new DateRange(start, end));
      }
      if (r1.end.isBefore(r2.end)) {
        i++;
      } else {
        j++;
      }
    }
    return result;
  }

  /**
   * Outputs availability map to JSON using pure Java StringBuilder approach.
   */
  public static String availabilityToJson(Map<String, List<DateRange>> availability) {
    if (availability.isEmpty()) return "{}";
    
    StringBuilder jsonBuilder = new StringBuilder("{\n");
    List<String> roomEntries = new ArrayList<>();
    
    for (var entry : availability.entrySet()) {
      StringBuilder roomJson = new StringBuilder();
      roomJson.append("  \"").append(escapeJsonString(entry.getKey())).append("\": [");
      
      List<String> rangeEntries = new ArrayList<>();
      for (var range : entry.getValue()) {
        rangeEntries.add(String.format("\n    {\"from\": \"%s\", \"to\": \"%s\"}", 
                                      range.start.toString(), range.end.toString()));
      }
      roomJson.append(String.join(",", rangeEntries));
      if (!rangeEntries.isEmpty()) {
        roomJson.append("\n  ");
      }
      roomJson.append("]");
      roomEntries.add(roomJson.toString());
    }
    
    jsonBuilder.append(String.join(",\n", roomEntries));
    jsonBuilder.append("\n}");
    return jsonBuilder.toString();
  }

  private static String escapeJsonString(String input) {
    if (input == null) return "";
    return input.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
  }

  public static void main(String[] args) {
    setupLogger();

    if (args.length < 3) {
      LOG.severe("Usage: java RoomAvailabilityCalendar bookings.csv maintenance.csv linkedRooms.csv");
      System.exit(1);
    }

    try (var bookingsReader = Files.newBufferedReader(Path.of(args[0]));
         var maintenanceReader = Files.newBufferedReader(Path.of(args[1]));
         var linkedRoomsReader = Files.newBufferedReader(Path.of(args[2]))) {

      List<Booking> bookings = parseBookings(bookingsReader);
      List<Maintenance> maintenance = parseMaintenance(maintenanceReader);
      List<LinkedRoomType> linkedRooms = parseLinkedRooms(linkedRoomsReader);

      var availability = calculateAvailability(bookings, maintenance, linkedRooms);
      String json = availabilityToJson(availability);
      System.out.println(json);

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "I/O Error: " + e.getMessage(), e);
      System.exit(1);
    } catch (IllegalStateException | IllegalArgumentException e) {
      LOG.log(Level.SEVERE, "Processing Error: " + e.getMessage(), e);
      System.exit(1);
    }
  }

  private static void setupLogger() {
    LOG.setUseParentHandlers(false);
    ConsoleHandler ch = new ConsoleHandler();
    ch.setLevel(Level.INFO);
    LOG.addHandler(ch);
    LOG.setLevel(Level.INFO);
  }
}
