package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public class MBDBPart2 {
    public static void main(String[]args){
        String testSelect = "SELECT * FROM property;";
        
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:5432/postgres", "postgres", "131")) {

            if (conn != null) {
                System.out.println("Connected to the database!");
            } else {
                System.out.println("Failed to make connection!");
            }
            
            Scanner scanner = new Scanner(System.in);
            String baseQuery = "SELECT * FROM preliminary WHERE action_taken_name = 'Loan originated' AND (";
            List<String> filters = new ArrayList<>();
            Map<String, String> activeFilters = new LinkedHashMap<>();
            while (true) {
                System.out.println("=== Mortgage Filter Program ===");
                System.out.println("Current Filters: " + activeFilters);
                System.out.println("""
                    1. Add a Filter
                    2. Delete a Specific Filter
                    3. Delete All Filters
                    4. Calculate and Offer Rate
                    5. Run Query
                    6. Exit
                    """);
                System.out.print("Choose an option: ");
                int choice = scanner.nextInt();
                scanner.nextLine();

                switch (choice) {
                    case 1:
                        addFilter(scanner, filters, activeFilters);
                        continue;
                    case 2:
                        deleteFilter(scanner, filters, activeFilters);
                        continue;
                    case 3:
                        filters.clear();
                        activeFilters.clear();
                        System.out.println("All filters cleared.");
                        continue;
                    case 4:
                        calculateAndOfferRate(conn, activeFilters);
                        continue;
                    case 5:
                     String finalQuery = baseQuery + String.join(" AND ", filters) + ");";
                     System.out.println("Executing Query: " + finalQuery);
                     executeQuery(conn, finalQuery);
                     continue;
                    
                    case 6:
                        System.out.println("Exiting...");
                        return;
                    default:
                        System.out.println("Invalid choice. Try again.");
                        continue;
                }
            }

        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

    
    private static void addFilter(Scanner scanner, List<String> filters, Map<String, String> activeFilters) {
        System.out.println("""
            Choose a filter to add:
            1. County
            2. Loan Type
            3. Tract to MSAMD Income
            4. Loan Purpose
            5. Property Type
            6. Owner Occupied
            """);
        System.out.print("Enter filter type: ");
        int filterType = scanner.nextInt();
        scanner.nextLine(); // Consume newline

        switch (filterType) {
            case 1 -> {
                System.out.println("Enter counties (comma-separated):");
                String counties = scanner.nextLine();
                filters.add("county_name IN (" + formatList(counties.split(",")) + ")");
                activeFilters.put("county_name", counties);
            }
            case 2 -> {
                System.out.println("Enter loan types (comma-separated):");
                String loanTypes = scanner.nextLine();
                filters.add("loan_type_name IN (" + formatList(loanTypes.split(",")) + ")");
                activeFilters.put("Loan Type", loanTypes);
            }
            case 3 -> {
                System.out.println("Enter minimum tract_to_msamd_income:");
                int min = scanner.nextInt();
                System.out.println("Enter maximum tract_to_msamd_income:");
                int max = scanner.nextInt();
                scanner.nextLine(); // Consume newline
                filters.add("tract_to_msamd_income BETWEEN " + min + " AND " + max);
                activeFilters.put("Tract to MSAMD Income", min + " - " + max);
            }
            case 4 -> {
                System.out.println("Enter loan purposes (comma-separated):");
                String loanPurposes = scanner.nextLine();
                filters.add("loan_purpose_name IN (" + formatList(loanPurposes.split(",")) + ")");
                activeFilters.put("Loan Purpose", loanPurposes);
            }
            case 5 -> {
                System.out.println("Enter property types (comma-separated):");
                String propertyTypes = scanner.nextLine();
                filters.add("property_type_name IN (" + formatList(propertyTypes.split(",")) + ")");
                activeFilters.put("Property Type", propertyTypes);
            }
            case 6 -> {
                System.out.println("Enter owner occupancy (Yes/No):");
                String ownerOccupied = scanner.nextLine();
                filters.add("owner_occupancy_name = '" + ownerOccupied + "'");
                activeFilters.put("Owner Occupied", ownerOccupied);
            }
            default -> System.out.println("Invalid filter type.");
        }
    }

    private static void deleteFilter(Scanner scanner, List<String> filters, Map<String, String> activeFilters) {
        if (activeFilters.isEmpty()) {
            System.out.println("No filters to delete.");
            return;
        }

        System.out.println("Current Filters:");
        int index = 1;
        Map<Integer, String> filterKeys = new HashMap<>();
        for (String key : activeFilters.keySet()) {
            System.out.println(index + ". " + key + " = " + activeFilters.get(key));
            filterKeys.put(index, key);
            index++;
        }

        System.out.print("Enter filter number to delete (or 0 to cancel): ");
        int filterChoice = scanner.nextInt();
        scanner.nextLine(); // Consume newline

        if (filterChoice == 0) {
            System.out.println("Deletion cancelled.");
            return;
        }

        String keyToRemove = filterKeys.get(filterChoice);
        if (keyToRemove != null) {
            filters.removeIf(filter -> filter.contains(keyToRemove));
            activeFilters.remove(keyToRemove);
            System.out.println("Filter '" + keyToRemove + "' removed.");
        } else {
            System.out.println("Invalid choice.");
        }
    }
    
    private static String formatList(String[] items) {
        return String.join(",", Arrays.stream(items).map(item -> "'" + item.trim() + "'").toArray(String[]::new));
    }

    private static void executeQuery(Connection conn, String query) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnName(i) + ": " + rs.getString(i) + " ");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    private static void calculateAndOfferRate(Connection conn, Map<String, String> activeFilters) {
        String baseQuery = "SELECT loan_amount_000s, rate_spread, lien_status, purchaser_type FROM preliminary WHERE action_taken_name = 'Loan originated' AND purchaser_type IN (0, 1, 2, 3, 4, 8)";
        
        if (!activeFilters.isEmpty()) {
            baseQuery += " AND " + activeFilters.entrySet().stream()
                    .map(entry -> entry.getKey() + " = '" + entry.getValue() + "'")
                    .collect(Collectors.joining(" AND "));
        }

        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(baseQuery)) {

            double baseRate = 2.33;
            double totalWeightedRate = 0.0;
            double totalLoanAmount = 0.0;

            while (rs.next()) {
                double loanAmount = rs.getDouble("loan_amount_000s");
                double rateSpread = rs.getDouble("rate_spread");
                int lienStatus = rs.getInt("lien_status");

                if (rs.wasNull()) { // Missing rate spread
                    if (lienStatus == 1) {
                        rateSpread = 1.5;
                    } else if (lienStatus == 2) {
                        rateSpread = 3.5;
                    }
                }

                double effectiveRate;
                if ((lienStatus == 1 && rateSpread < 1.5) || (lienStatus == 2 && rateSpread < 3.5)) {
                    effectiveRate = Double.NaN; // NA
                } else {
                    effectiveRate = baseRate + rateSpread;
                }

                if (!Double.isNaN(effectiveRate)) {
                    totalWeightedRate += effectiveRate * loanAmount;
                    totalLoanAmount += loanAmount;
                }
            }

            if (totalLoanAmount == 0) {
                System.out.println("No valid mortgages found for rate calculation.");
                return;
            }

            double weightedAverageRate = totalWeightedRate / totalLoanAmount;
            System.out.printf("Weighted Average Rate: %.2f%%\n", weightedAverageRate);
            System.out.printf("Total Cost of Securitization: $%.2f\n", totalLoanAmount * 1000);

            System.out.print("Do you accept this rate and total cost? (yes/no): ");
            Scanner scanner = new Scanner(System.in);
            String response = scanner.nextLine();

            if (response.equalsIgnoreCase("yes")) {
                updatePurchaserType(conn, activeFilters);
            } else {
                System.out.println("Rate and cost declined. Returning to the main menu.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    private static void updatePurchaserType(Connection conn, Map<String, String> activeFilters) {
        String updateQuery = "UPDATE preliminary SET purchaser_type = 9, purchaser_type_name = 'Private Securitization' " +
                             "WHERE action_taken_name = 'Loan originated' AND purchaser_type IN (0, 1, 2, 3, 4, 8)";
        
        if (!activeFilters.isEmpty()) {
            updateQuery += " AND " + activeFilters.entrySet().stream()
                    .map(entry -> entry.getKey() + " = '" + entry.getValue() + "'")
                    .collect(Collectors.joining(" AND "));
        }
    
        try (Statement stmt = conn.createStatement()) {
            int rowsUpdated = stmt.executeUpdate(updateQuery);
            System.out.println(rowsUpdated + " mortgages updated to 'Private Securitization'.");
            System.exit(0); // Exit the program after a successful update
        } catch (SQLException e) {
            System.out.println("Error updating purchaser types. Returning to the main menu.");
            e.printStackTrace();
        }
    }
    
}
