package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by omkar on 5/5/17.
 */

public class Node {
    String portNumber;
    String portHashID;
    int address;

    /*
    Constructor: (pNumber and pHashID) and 0 parameter
     */

    public Node() {
    }

    /*
    Getter and Setter
     */
    public int getAddress() {
        return address;
    }

    public void setAddress(int address) {
        this.address = address;
    }

    public String getPortNumber() {
        return portNumber;
    }

    public void setPortNumber(String portNumber) {
        this.portNumber = portNumber;
    }

    public String getPortHashID() {
        return portHashID;
    }

    public void setPortHashID(String portHashID) {
        this.portHashID = portHashID;
    }
}
