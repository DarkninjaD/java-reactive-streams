package com.github.DarkninjaD.Models;

/**
 * DataDTO -
 * The Object that we will convert everything too
 * so we can do some work with it downstream.
 *
 * @param title
 * @param description
 * @param runtime
 */
public record DataDTO(
    String title,
    String description,
    Number runtime) {
};