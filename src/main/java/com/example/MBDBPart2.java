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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

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
                    6. Add mortgage
                    7. Exit
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
                        addNewMortgage(conn, scanner);
                        continue;
                    case 7:
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
            Set<String> columnsToSkip = new HashSet<>();
    
            // First pass: Identify columns to skip
            Map<String, String> baseToNameColumn = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (columnName.endsWith("_name")) {
                    // Extract base name
                    String baseName = columnName.substring(0, columnName.lastIndexOf("_name"));
                    baseToNameColumn.put(baseName, columnName);
                }
            }
    
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (!columnName.endsWith("_name")) {
                    String baseName = columnName.contains("_")
                            ? columnName.substring(0, columnName.lastIndexOf('_'))
                            : columnName;
                    // Skip columns that have a corresponding _name column
                    if (baseToNameColumn.containsKey(baseName)) {
                        columnsToSkip.add(columnName);
                    }
                }
            }
    
            // Second pass: Print only non-skipped columns and calculate row count and loan sum
            int rowCount = 0;
            double loanAmountSum = 0.0;
            while (rs.next()) {
                rowCount++;
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    if (!columnsToSkip.contains(columnName)) {
                        System.out.print(columnName + ": " + rs.getString(i) + " ");
                    }
                    // Accumulate loan amount sum if column is loan_amount_000s
                    if (columnName.equals("loan_amount_000s")) {
                        loanAmountSum += rs.getDouble(i);
                    }
                }
                System.out.println();
            }
    
            // Print row count and loan amount sum
            System.out.println("\nNumber of rows matching the filters: " + rowCount);
            System.out.println("Total loan amount (in 000s): " + loanAmountSum);
    
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
    
                if (key.equals("tract_to_msamd_income_min") || key.equals("tract_to_msamd_income_max")) {
                    continue; // Skip these keys; we'll handle them as a range.
                }
    
                if (value.contains(",")) {
                    String[] range = value.split(",");
                    if (isNumericColumn(range[0]) && isNumericColumn(range[1])) {
                        baseQuery.append(key).append(" BETWEEN ? AND ? AND ");
                        parameters.add(Double.parseDouble(range[0]));
                        parameters.add(Double.parseDouble(range[1]));
                    }
                } else {
                    if (isNumericColumn(value)) {
                        baseQuery.append(key).append(" = ? AND ");
                        parameters.add(Double.parseDouble(value));
                    } else {
                        baseQuery.append(key).append(" = ? AND ");
                        parameters.add(value);
                    }
                }
            }
    
            // Handle tract_to_msamd_income range if applicable
            if (activeFilters.containsKey("tract_to_msamd_income_min") && activeFilters.containsKey("tract_to_msamd_income_max")) {
                baseQuery.append("tract_to_msamd_income BETWEEN ? AND ? AND ");
                parameters.add(Double.parseDouble(activeFilters.get("tract_to_msamd_income_min")));
                parameters.add(Double.parseDouble(activeFilters.get("tract_to_msamd_income_max")));
            }
    
            baseQuery.setLength(baseQuery.length() - 5); // Trim trailing AND
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
    
    private static void performTransactionalUpdate(Connection conn, Map<String, String> activeFilters) {
        try {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
    
            String updateQuery = "UPDATE preliminary2 SET purchaser_type = 9, purchaser_type_name = 'Private Securitization' " +
                    "WHERE action_taken_name = 'Loan originated' AND purchaser_type IN (0, 1, 2, 3, 4, 8)";
    
            List<Object> parameters = new ArrayList<>();
            if (!activeFilters.isEmpty()) {
                updateQuery += " AND ";
                for (Map.Entry<String, String> entry : activeFilters.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
    
                    if (key.equals("tract_to_msamd_income_min") || key.equals("tract_to_msamd_income_max")) {
                        continue; // Skip these; handle them as a range below.
                    }
    
                    // Determine whether the column is numeric or string
                    if (isNumericColumn(key)) {
                        updateQuery += key + " = ? AND ";
                        parameters.add(Double.parseDouble(value));
                    } else {
                        updateQuery += key + " = ? AND ";
                        parameters.add(value);
                    }
                }
    
                // Handle the tract_to_msamd_income range separately
                if (activeFilters.containsKey("tract_to_msamd_income_min") && activeFilters.containsKey("tract_to_msamd_income_max")) {
                    updateQuery += "tract_to_msamd_income BETWEEN ? AND ? AND ";
                    parameters.add(Double.parseDouble(activeFilters.get("tract_to_msamd_income_min")));
                    parameters.add(Double.parseDouble(activeFilters.get("tract_to_msamd_income_max")));
                }
    
                updateQuery = updateQuery.substring(0, updateQuery.length() - 5); // Remove trailing AND
            }
    
            try (PreparedStatement stmt = conn.prepareStatement(updateQuery)) {
                for (int i = 0; i < parameters.size(); i++) {
                    stmt.setObject(i + 1, parameters.get(i));
                }
    
                int rowsUpdated = stmt.executeUpdate();
                if (rowsUpdated > 0) {
                    conn.commit();
                    System.out.println(rowsUpdated + " mortgages updated to 'Private Securitization'. Transaction committed.");
                } else {
                    conn.rollback();
                    System.out.println("No matching rows were updated. Transaction rolled back.");
                }
            }
        } catch (SQLException e) {
            try {
                conn.rollback();
                System.out.println("Error during transaction. Changes rolled back.");
                e.printStackTrace();
            } catch (SQLException rollbackEx) {
                rollbackEx.printStackTrace();
            }
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    
    
    
    /**
 * Determine if the column is numeric based on known numeric columns.
 * Update this list if additional numeric columns are added to the database schema.
 */
    private static boolean isNumericColumn(String columnName) {
        List<String> numericColumns = Arrays.asList(
            "tract_to_msamd_income", "loan_amount_000s", "applicant_income_000s",
            "rate_spread", "lien_status", "purchaser_type"
        );
        return numericColumns.contains(columnName);
    }

    
    private static void addNewMortgage(Connection conn, Scanner scanner) {
        try {
            System.out.println("Adding a New Mortgage to the Database...");
            
            // Prompt user for all necessary fields
            System.out.print("Enter applicant income (000s): ");
            double income = scanner.nextDouble();
            System.out.print("Enter loan amount (000s): ");
            double loanAmount = scanner.nextDouble();
            scanner.nextLine(); // Clear newline buffer
            
            System.out.println("Choose an MSAMD option: ");
            System.out.println("""
                    1. Allentown, Bethlehem, Easton - PA, NJ
                    2. Atlantic City, Hammonton - NJ
                    3. Camden - NJ
                    4. New York, Jersey City, White Plains - NY, NJ
                    5. Newark - NJ, PA
                    6. Ocean City - NJ
                    7. Trenton - NJ
                    8. Vineland, Bridgeton - NJ
                    9. Wilmington - DE, MD, NJ
                    """);
            int msamdNum = scanner.nextInt();
            String msamd = null;
            switch(msamdNum){
                case 1: msamd = "Allentown, Bethlehem, Easton - PA, NJ"; break;
                case 2: msamd = "Atlantic City, Hammonton - NJ";break;
                case 3: msamd = "Camden - NJ";break;
                case 4: msamd = "New York, Jersey City, White Plains - NY, NJ";break;
                case 5: msamd = "Newark - NJ, PA";break;
                case 6: msamd = "Ocean City - NJ";break;
                case 7: msamd = "Trenton - NJ";break;
                case 8: msamd = "Vineland, Bridgeton - NJ";break;
                case 9: msamd = "Wilmington - DE, MD, NJ";break;
                default: msamd = null;
            }
            String county_name = msamd.split("\\s+")[0];
            
            System.out.println("Choose applicant sex: ");
            System.out.println("""
                    1. Female
                    2. Male
                    3. Not applicable
                    4. Do not wish to provide
                   """);
            int applicantSexNum = scanner.nextInt();
            String applicantSex = null;
            switch(applicantSexNum){
                case 1: applicantSex = "Female";break;
                case 2: applicantSex = "Male";break;
                case 3: applicantSex = "Not applicable";break;
                case 4: applicantSex = "Information not provided by applicant in mail, Internet, or telephone application";break;
                default: applicantSex = null;
            }
            
            System.out.println("Choose loan type: ");
            System.out.println("""
                    1. Conventional
                    2. FHA-insured
                    3. FSA/RHS-guaranteed
                    4. VA-guaranteed
                   """);
            int loanTypeNum = scanner.nextInt();
            String loanType = null;
            switch(loanTypeNum){
                case 1: loanType = "Conventional";break;
                case 2: loanType = "FHA-insured";break;
                case 3: loanType = "FSA/RHS-guaranteed";break;
                case 4: loanType = "VA-guaranteed";break;
                default: loanType = null;
            }
            
            System.out.println("Choose applicant ethnicity: ");
            System.out.println("""
                    1. Hispanic or Latino
                    2. Not Hispanic or Latino
                    3. Not applicable
                    4. Don't wish to answer
                   """);
            int ethnicityNum = scanner.nextInt();
            String ethnicity = null;
            switch(ethnicityNum){
                case 1: ethnicity = "Hispanic or Latino";break;
                case 2: ethnicity = "Not Hispanic or Latino";break;
                case 3: ethnicity = "Not applicable";break;
                case 4: ethnicity = "Information not provided by applicant in mail, Internet, or telephone application";break;
                default: ethnicity = null;
            }

            // Use a SQL insert query with placeholders
            String insertQuery = 
                "INSERT INTO preliminary2 (applicant_income_000s, loan_amount_000s, msamd_name, applicant_sex_name, loan_type_name, applicant_ethnicity_name, county_name) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?);";

            try (PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {
                pstmt.setDouble(1, income);
                pstmt.setDouble(2, loanAmount);
                pstmt.setString(3, msamd); 
                pstmt.setString(4, applicantSex);
                pstmt.setString(5, loanType);
                pstmt.setString(6, ethnicity);
                pstmt.setString(7, county_name);

                int rowsInserted = pstmt.executeUpdate();
                if (rowsInserted > 0) {
                    System.out.println("Executing: " + pstmt);
                    System.out.println("Mortgage added successfully!");
                } else {
                    System.out.println("Failed to add mortgage.");
                }
            }
        } catch (SQLException e) {
            System.err.println("Error while inserting mortgage into the database.");
            e.printStackTrace();
        }
    }
    
}
