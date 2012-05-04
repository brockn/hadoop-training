package com.cloudera.training.hadoop.sssp1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class Node implements Writable, Cloneable {
  private String[] friends;
  private int distance;

  public Node() {
    this(-1);
  }

  public Node(int distance) {
    this(distance, (String[])null);
  }

  public Node(int distance, String ... friends) {
    this.distance = distance;
    this.friends = friends;
    if(this.friends != null && this.friends.length > 0) {
      Arrays.sort(this.friends);
    }
  }

  @Override
  public Node clone() {
    Node node = new Node(distance, Arrays.copyOf(friends, friends.length));
    return node;
  }
  @Override
  public boolean equals(Object obj) {
    Node node = (Node) obj;
    if (distance == node.distance) {
      if (friends == null && node.friends == null) {
        return true;
      } else if (friends != null && node.friends != null) {
        return Arrays.equals(friends, node.friends);
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return distance;
  }

  public void setDistance(int distance) {
    this.distance = distance;
  }

  public String[] getFriends() {
    return friends;
  }

  public int getDistance() {
    return distance;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    if (friends != null) {
      for (int i = 0; i < friends.length; i++) {
        buffer.append(friends[i]).append(",");
      }
    }
    buffer.append(distance);
    return buffer.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (friends == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(friends.length);
      for (int i = 0; i < friends.length; i++) {
        out.writeUTF(friends[i]);
      }
    }
    out.writeInt(distance);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numFriends = in.readInt();
    if (numFriends > 0) {
      friends = new String[numFriends];
      for (int i = 0; i < numFriends; i++) {
        friends[i] = in.readUTF();
      }
      Arrays.sort(friends);
    } else {
      // Set to null as this object will be reused
      friends = null;
    }
    distance = in.readInt();
  }
}