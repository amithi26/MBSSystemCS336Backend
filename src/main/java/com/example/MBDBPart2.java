package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
            String baseQuery = "SELECT * FROM preliminary2 WHERE action_taken_name = 'Loan originated' AND (";
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
                     if(finalQuery.equals("SELECT * FROM preliminary2 WHERE action_taken_name = 'Loan originated' AND ();")){
                        finalQuery = "SELECT * FROM preliminary2 WHERE action_taken_name = 'Loan originated'";
                     }
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
            7. MSAMD
            8. Income to Debt Ratio
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
                activeFilters.put("loan_type_name", loanTypes);
            }
            case 3 -> {
                System.out.println("Enter minimum tract_to_msamd_income:");
                double min = scanner.nextDouble();
                System.out.println("Enter maximum tract_to_msamd_income:");
                double max = scanner.nextDouble();
                scanner.nextLine(); // Consume newline
                // Fix the invalid syntax issue by formatting the filter properly.
                filters.add("tract_to_msamd_income >= " + min + " AND tract_to_msamd_income <= " + max);
                activeFilters.put("tract_to_msamd_income", min + "," + max); // Store for later query use
            }
            
            case 4 -> {
                System.out.println("Enter loan purposes (comma-separated):");
                String loanPurposes = scanner.nextLine();
                filters.add("loan_purpose_name IN (" + formatList(loanPurposes.split(",")) + ")");
                activeFilters.put("loan_purpose_name", loanPurposes);
            }
            case 5 -> {
                System.out.println("Enter property types (comma-separated):");
                String propertyTypes = scanner.nextLine();
                filters.add("property_type_name IN (" + formatList(propertyTypes.split(",")) + ")");
                activeFilters.put("property_type_name", propertyTypes);
            }
            case 6 -> {
                System.out.println("Enter owner occupancy (options: Not owner-occupied as a principal dwelling, Owner-occupied as a principal dwelling):");
                String ownerOccupied = scanner.nextLine();
                filters.add("owner_occupancy_name = '" + ownerOccupied + "'");
                activeFilters.put("owner_occupancy_name", ownerOccupied);
            }
            case 7 -> {
                System.out.println("Enter valid MSAMD:");
                String msamd = scanner.nextLine();
                filters.add("msamd_name = '" + msamd + "'");
                activeFilters.put("msamd_name", msamd);
            }
            case 8 -> {
                System.out.println("Enter minimum applicant income to loan amount ratio (or press Enter to skip):");
                String minRatioInput = scanner.nextLine();
                System.out.println("Enter maximum applicant income to loan amount ratio (or press Enter to skip):");
                String maxRatioInput = scanner.nextLine();

                if (!minRatioInput.isEmpty() || !maxRatioInput.isEmpty()) {
                    List<String> conditions = new ArrayList<>();
                    if (!minRatioInput.isEmpty()) {
                        double minRatio = Double.parseDouble(minRatioInput);
                        conditions.add("(applicant_income_000s / loan_amount_000s) >= " + minRatio);
                    }
                    if (!maxRatioInput.isEmpty()) {
                        double maxRatio = Double.parseDouble(maxRatioInput);
                        conditions.add("(applicant_income_000s / loan_amount_000s) <= " + maxRatio);
                    }
                    // Join the conditions with "AND" if both min and max are provided
                    String condition = String.join(" AND ", conditions);
                    filters.add(condition);
                    activeFilters.put("income_to_loan_ratio", 
                        (!minRatioInput.isEmpty() ? "Min: " + minRatioInput : "") +
                        (!minRatioInput.isEmpty() && !maxRatioInput.isEmpty() ? ", " : "") +
                        (!maxRatioInput.isEmpty() ? "Max: " + maxRatioInput : "")
                    );
                } else {
                    System.out.println("No ratio filter applied.");
                }
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
            // Remove from activeFilters
            String valueToRemove = activeFilters.remove(keyToRemove);
    
            // Build the SQL-like filter string to remove from `filters`
            String filterStringToRemove = buildFilterString(keyToRemove, valueToRemove);
            if (filterStringToRemove != null) {
                filters.remove(filterStringToRemove);
                System.out.println("Filter '" + keyToRemove + "' removed.");
            } else {
                System.out.println("Error: Could not build filter string to remove.");
            }
        } else {
            System.out.println("Invalid choice.");
        }
        
        // Debugging: Print the current filters list
        System.out.println("Filters list after deletion: " + filters);
    }
    
    // Helper method to build the filter string based on key and value
    private static String buildFilterString(String key, String value) {
        switch (key) {
            case "county_name":
                return "county_name IN (" + formatList(value.split(",")) + ")";
            case "loan_type_name":
                return "loan_type_name IN (" + formatList(value.split(",")) + ")";
            case "tract_to_msamd_income": {
                String[] incomeRange = value.split(",");
                if (incomeRange.length == 2) {
                    return "tract_to_msamd_income BETWEEN " + incomeRange[0].trim() + " AND " + incomeRange[1].trim();
                } else {
                    throw new IllegalArgumentException("Invalid range for tract_to_msamd_income: " + value);
                }
            }  
            case "loan_purpose_name":
                return "loan_purpose_name IN (" + formatList(value.split(",")) + ")";
            case "property_type_name":
                return "property_type_name IN (" + formatList(value.split(",")) + ")";
            case "owner_occupancy_name":
                return "owner_occupancy_name = '" + value + "'";
            case "msamd_name":
                return "msamd_name = '" + value + "'";
            case "income_to_loan_ratio":
                String[] parts = value.split(", ");
                List<String> conditions = new ArrayList<>();
                for (String part : parts) {
                    if (part.startsWith("Min:")) {
                        conditions.add("(applicant_income_000s / loan_amount_000s) >= " + part.substring(5));
                    } else if (part.startsWith("Max:")) {
                        conditions.add("(applicant_income_000s / loan_amount_000s) <= " + part.substring(5));
                    }
                }
                return String.join(" AND ", conditions);
            default:
                return null;
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
        StringBuilder baseQuery = new StringBuilder(
                "SELECT loan_amount_000s, rate_spread, lien_status, purchaser_type FROM preliminary2 WHERE action_taken_name = 'Loan originated' AND purchaser_type IN (0, 1, 2, 3, 4, 8)");
    
        List<Object> parameters = new ArrayList<>();
    
        if (!activeFilters.isEmpty()) {
            baseQuery.append(" AND ");
            for (Map.Entry<String, String> entry : activeFilters.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
    
                if (value.contains(",")) {
                    String[] range = value.split(",");
                    if (isNumeric(range[0]) && isNumeric(range[1])) {
                        baseQuery.append(key).append(" BETWEEN ? AND ? AND ");
                        parameters.add(Double.parseDouble(range[0]));
                        parameters.add(Double.parseDouble(range[1]));
                    }
                } else {
                    if (isNumeric(value)) {
                        baseQuery.append(key).append(" = ? AND ");
                        parameters.add(Double.parseDouble(value));
                    } else {
                        baseQuery.append(key).append(" = ? AND ");
                        parameters.add(value);
                    }
                }
            }
            baseQuery.setLength(baseQuery.length() - 5);
        }
    
        try (PreparedStatement stmt = conn.prepareStatement(baseQuery.toString())) {
            for (int i = 0; i < parameters.size(); i++) {
                stmt.setObject(i + 1, parameters.get(i));
            }
    
            try (ResultSet rs = stmt.executeQuery()) {
                double baseRate = 2.33;
                double totalWeightedRate = 0.0; 
                double totalLoanAmount = 0.0; 
    
                while (rs.next()) {
                    double loanAmount = rs.getDouble("loan_amount_000s");
                    double rateSpread = rs.getDouble("rate_spread");
                    int lienStatus = rs.getInt("lien_status");
    
                    totalLoanAmount += loanAmount; 
    
                    if ((lienStatus == 1 && rateSpread < 1.5) || (lienStatus == 2 && rateSpread < 3.5)) {
                        continue;
                    }
    
                    totalWeightedRate += loanAmount * rateSpread; 
                }
    
                double weightedRateSpread = totalLoanAmount > 0 ? (totalWeightedRate / totalLoanAmount) : 0;
                double finalRate = baseRate + weightedRateSpread;
    
                System.out.println("Final Weighted Rate Spread Calculation: " + finalRate);
                System.out.println("Total loan amount (cost of securitization): $" + totalLoanAmount);
    
                System.out.println("Do you accept this rate? (yes/no): ");
                Scanner scanner = new Scanner(System.in);
                String userChoice = scanner.nextLine();
    
                if (userChoice.equalsIgnoreCase("yes")) {
                    performTransactionalUpdate(conn, activeFilters);
                    System.exit(0);
                } else {
                    System.out.println("Rate rejected. Returning to main menu...");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Perform the database transaction for updating the purchaser type with SERIALIZABLE isolation
     */
    private static void performTransactionalUpdate(Connection conn, Map<String, String> activeFilters) {
        try {
            // Set connection to SERIALIZABLE isolation level
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false); // Start transaction manually
    
            // Perform the database update
            String updateQuery = "UPDATE preliminary2 SET purchaser_type = 9, purchaser_type_name = 'Private Securitization' " +
                    "WHERE action_taken_name = 'Loan originated' AND purchaser_type IN (0, 1, 2, 3, 4, 8)";
    
            if (!activeFilters.isEmpty()) {
                updateQuery += " AND " + activeFilters.entrySet().stream()
                        .map(entry -> entry.getKey() + " = '" + entry.getValue() + "'")
                        .collect(Collectors.joining(" AND "));
            }
    
            try (PreparedStatement stmt = conn.prepareStatement(updateQuery)) {
                int rowsUpdated = stmt.executeUpdate();
                if (rowsUpdated > 0) {
                    conn.commit(); // Commit the transaction if successful
                    System.out.println(rowsUpdated + " mortgages updated to 'Private Securitization'. Transaction committed.");
                } else {
                    conn.rollback();
                    System.out.println("No matching rows were updated. Transaction rolled back.");
                }
            }
    
        } catch (SQLException e) {
            try {
                conn.rollback();
                System.out.println("Error occurred during transaction. Changes rolled back.");
                e.printStackTrace();
            } catch (SQLException rollbackEx) {
                System.err.println("Rollback failed.");
                rollbackEx.printStackTrace();
            }
        } finally {
            try {
                conn.setAutoCommit(true); // Restore the default behavior for further operations
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    private static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    
    
}
