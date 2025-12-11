package cis5550.jobs;

import cis5550.flame.FlameContext;

public class ClearTables {
  public static void run(FlameContext ctx, String[] args) throws Exception {
    String[] tables = {"pt-crawl", "pt-content", "pt-index", "pt-pageranks"};
    for (String t : tables) {
      try {
        ctx.getKVS().delete(t);
        ctx.output("Deleted table: " + t);
      } catch (Exception e) {
        ctx.output("Failed to delete " + t + ": " + e.getMessage());
      }
    }
  }
}