package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	String fileName;

	/*
	 * parameterized constructor to initialize filename. As you are trying to
	 * perform file reading, hence you need to be ready to handle the IO
	 * Exceptions.
	 */
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
	}

	/*
	 * implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 */
	@Override
	public Header getHeader() throws IOException {
		// TODO Auto-generated method stub
		FileReader fileReader = new FileReader(fileName);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String headerRow = bufferedReader.readLine();
		bufferedReader.close();
		fileReader.close();

		String[] headers = headerRow.split(",");
		Header header = new Header();
		header.setHeaders(headers);
		return header;
	}

	/**
	 * This method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {

	}

	/*
	 * implementation of getColumnType() method. To find out the data types, we
	 * will read the first line from the file and extract the field values from
	 * it. In the previous assignment, we have tried to convert a specific field
	 * value to Integer or Double. However, in this assignment, we are going to
	 * use Regular Expression to find the appropriate data type of a field.
	 * Integers: should contain only digits without decimal point Double: should
	 * contain digits as well as decimal point Date: Dates can be written in
	 * many formats in the CSV file. However, in this assignment,we will test
	 * for the following date formats('dd/mm/yyyy',
	 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','
	 * yyyy-mm-dd')
	 */
	@Override
	public DataTypeDefinitions getColumnType() throws FileNotFoundException {
	//	String[] colunmType = null;
		List<String> recordColumns = new ArrayList<String>();
		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))) {
			int columnNumbers = bufferedReader.readLine().split(",").length;

			String columnHeader = bufferedReader.readLine();
			String columnHeaders[] = columnHeader.split(",", -1);
			//colunmType = new String[columnNumbers];
			int index = 0;
			for (String recordcolumn : columnHeaders) {

				// checking for Integer
				try {
					int recValue = Integer.parseInt(recordcolumn);
					Object object = (Object) recValue;
					recordColumns.add(object.getClass().getName());
				} catch (NumberFormatException numberFormatException) {
					try {
						double recValue = Double.parseDouble(recordcolumn);
						Object object = (Object) recValue;
						recordColumns.add(object.getClass().getName());
					} catch (NumberFormatException numberFormatException2) {
						if (recordcolumn.matches("^\\d{1,2}\\/\\d{1,2}\\/\\d{4}$")
								|| recordcolumn.matches("^\\d{1,2}\\-\\[A-Za-z]{3}\\-\\d{2}$")
								|| recordcolumn.matches("^\\d{1,2}\\-\\[A-Za-z]{3}\\-\\d{4}$")
								|| recordcolumn.matches("^\\d{1,2}\\-\\[A-Za-z]{5}\\-\\d{2}$")
								|| recordcolumn.matches("^\\d{1,2}\\-\\[A-Za-z]{5}\\-\\d{4}$")
								|| recordcolumn.matches("^\\d{4}\\-\\d{1,2}\\-\\d{1,2}$")) {
							recordColumns.add("java.util.Date");
						} else if (recordcolumn.matches("^\\w[a-zA-Z_0-9]*.*$")) {
							recordColumns.add("String".getClass().getName());
						} else
							recordColumns.add("java.lang.Object");
					}
				}
			}
		} catch (FileNotFoundException fileNotFoundException) {
			fileNotFoundException.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String columnDataType[] = new String[recordColumns.size()];

		recordColumns.toArray(columnDataType);
		DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions();
		dataTypeDefinitions.setDataTypes(columnDataType);
		return dataTypeDefinitions;
	}
}