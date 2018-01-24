package com.stackroute.datamunger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.reader.CsvQueryProcessor;

public class DataMunger {

	public static void main(String[] args) throws FileNotFoundException {
		String fileName = new Scanner(System.in).nextLine();

		// read the file name from the user
		/*
		 * create object of CsvQueryProcessor. We are trying to read from a file
		 * inside the constructor of this class. Hence, we will have to handle
		 * exceptions.
		 */

		CsvQueryProcessor csvQueryProcessor = new CsvQueryProcessor(fileName);
		// call getHeader() method to get the array of headers

		try {
			Header header = csvQueryProcessor.getHeader();
			DataTypeDefinitions dataTypeDefinitions = csvQueryProcessor.getColumnType();
			String[] columnHeaders = header.getHeaders();
			String[] columnDataTypes = dataTypeDefinitions.getDataTypes();
		} catch (IOException ioException) {
			ioException.printStackTrace();
		}

		/*
		 * call getColumnType() method of CsvQueryProcessor class to retrieve
		 * the array of column data types which is actually the object of
		 * DataTypeDefinitions class
		 */

		/*
		 * display the columnName from the header object along with its data
		 * type from DataTypeDefinitions object
		 */
	}
}