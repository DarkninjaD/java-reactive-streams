package com.github.DarkninjaD.Models;

import java.time.Year;

/**
 * DataDTO - The Object that we will convert everything too so we can do some
 * work with it
 * downstream.
 *
 * @param String title,
 * @param Number runtime,
 * @param Year yearMade,
 * @param StringArray genres,
 * @param String source
 */
public record MovieDTO(
  String title,
  Number runtime,
  Year yearMade,
  //String[] genres,
  String source
) {}
